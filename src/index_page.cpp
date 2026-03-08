#include "index_page.h"
#include "logging.h"
#include <algorithm>
#include <vector>

bool LeafIndexPage::hasKey(int key) const
{
    for (int idx = 0; idx < page_.getSlotCount(); ++idx)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(idx);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }
        LeafCell cell = page_.getLeafCellOnXthPointer(idx);
        if (cell.key() == key)
        {
            return true;
        }
    }
    return false;
}

std::optional<std::pair<uint16_t, uint16_t>> LeafIndexPage::findRef(int key, bool do_invalidate)
{
    /**
     * PERFORMANCE:Same consideration as Page::findLeafRef; kept verbatim.
     */
    for (int idx = 0; idx < page_.getSlotCount(); ++idx)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(idx);
        if (!Cell::isValid(cell_data))
        {
            LOG_DEBUG("LeafIndexPage::findRef skipping invalid slot {}", idx);
            continue;
        }
        LeafCell cell = page_.getLeafCellOnXthPointer(idx);
        if (cell.key() == key)
        {
            if (do_invalidate)
            {
                LOG_DEBUG("LeafIndexPage::findRef invalidating slot {} for key {}", idx, key);
                page_.invalidateSlot(idx);
            }
            return std::make_pair(cell.heap_page_id(), cell.slot_id());
        }
    }
    LOG_INFO("LeafIndexPage::findRef key {} not found in this page.", key);
    return std::nullopt;
}

void LeafIndexPage::transferAndCompactTo(LeafIndexPage &dst, char *separate_key)
{
    int separate_key_value = LeafCell::getKey(separate_key);

    const int slot_count = page_.getSlotCount();
    for (int idx = 0; idx < slot_count; ++idx)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(idx);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }

        int cell_key = LeafCell::getKey(cell_data);
        if (cell_key <= separate_key_value)
        {
            dst.page_.insertCell(LeafCell::decodeCell(cell_data));
            page_.invalidateSlot(idx);
        }
    }

    // Compact this page (leaf branch of Page::compact).
    uint8_t old_slot_count = page_.getSlotCount();

    std::vector<LeafCell> cells;
    cells.reserve(old_slot_count);
    for (int i = 0; i < old_slot_count; ++i)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(i);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }
        cells.push_back(page_.getLeafCellOnXthPointer(i));
    }

    const uint8_t new_slot_count = static_cast<uint8_t>(cells.size());
    if (new_slot_count == 0)
    {
        throw std::logic_error("LeafIndexPage::transferAndCompactTo: new_slot_count == 0 (not implemented)");
    }

    uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
    for (uint8_t idx = 0; idx < new_slot_count; ++idx)
    {
        const LeafCell &cell = cells[idx];
        const uint16_t payload_size = static_cast<uint16_t>(cell.payloadSize());
        write_offset = static_cast<uint16_t>(write_offset - payload_size);
        char *dest = page_.start_p_ + write_offset;
        std::vector<std::byte> serialized = cell.serialize();
        std::memcpy(dest, serialized.data(), serialized.size());

        char *slot_ptr_p = page_.start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
        std::memcpy(slot_ptr_p, &write_offset, sizeof(uint16_t));
    }

    page_.updateSlotCount(new_slot_count);
    page_.updateSlotDirectoryOffset(write_offset);
    page_.markDirty();

    LOG_INFO("Completed transfer and compaction of LeafIndexPage. New slot count: {}, new slot directory offset: {}", new_slot_count, write_offset);
}

uint16_t InternalIndexPage::findChildPage(int key)
{
    // collect valid intermediate cells.
    std::vector<IntermediateCell> cells;
    cells.reserve(page_.getSlotCount());
    for (int idx = 0; idx < page_.getSlotCount(); ++idx)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(idx);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }
        cells.push_back(page_.getIntermediateCellOnXthPointer(idx));
    }

    std::sort(cells.begin(), cells.end(), [](const IntermediateCell &a, const IntermediateCell &b) {
        return a.key() < b.key();
    });

    for (const IntermediateCell &cell : cells)
    {
        if (cell.key() >= key)
        {
            return cell.page_id();
        }
    }
    LOG_INFO("InternalIndexPage::findChildPage: all keys in this page are smaller than key {}. Going to right most child {}.",
             key, page_.rightMostChildPageId());
    return page_.rightMostChildPageId();
}

void InternalIndexPage::transferAndCompactTo(InternalIndexPage &dst, char *separate_key)
{
    int separate_key_value = IntermediateCell::getKey(separate_key);

    const int slot_count = page_.getSlotCount();
    for (int idx = 0; idx < slot_count; ++idx)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(idx);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }

        int cell_key = IntermediateCell::getKey(cell_data);
        if (cell_key <= separate_key_value)
        {
            dst.page_.insertCell(IntermediateCell::decodeCell(cell_data));
            page_.invalidateSlot(idx);
        }
    }

    // Compact this page for intermediate cells (analogous to leaf compaction).
    uint8_t old_slot_count = page_.getSlotCount();

    std::vector<IntermediateCell> cells;
    cells.reserve(old_slot_count);
    for (int i = 0; i < old_slot_count; ++i)
    {
        char *cell_data = page_.start_p_ + page_.getCellOffsetOnXthPointer(i);
        if (!Cell::isValid(cell_data))
        {
            continue;
        }
        cells.push_back(page_.getIntermediateCellOnXthPointer(i));
    }

    const uint8_t new_slot_count = static_cast<uint8_t>(cells.size());
    if (new_slot_count == 0)
    {
        throw std::logic_error("InternalIndexPage::transferAndCompactTo: new_slot_count == 0 (not implemented)");
    }

    uint16_t write_offset = static_cast<uint16_t>(Page::PAGE_SIZE_BYTE);
    for (uint8_t idx = 0; idx < new_slot_count; ++idx)
    {
        const IntermediateCell &cell = cells[idx];
        const uint16_t payload_size = static_cast<uint16_t>(cell.payloadSize());
        write_offset = static_cast<uint16_t>(write_offset - payload_size);
        char *dest = page_.start_p_ + write_offset;
        std::vector<std::byte> serialized = cell.serialize();
        std::memcpy(dest, serialized.data(), serialized.size());

        char *slot_ptr_p = page_.start_p_ + Page::HEADDER_SIZE_BYTE + Page::CELL_POINTER_SIZE * idx;
        std::memcpy(slot_ptr_p, &write_offset, sizeof(uint16_t));
    }

    page_.updateSlotCount(new_slot_count);
    page_.updateSlotDirectoryOffset(write_offset);
    page_.markDirty();

    LOG_INFO("Completed transfer and compaction of InternalIndexPage. New slot count: {}, new slot directory offset: {}", new_slot_count, write_offset);
}
