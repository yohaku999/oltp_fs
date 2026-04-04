#include "index_scan.h"
#include "logging.h"
#include "../storage/btreecursor.h"

IndexLookup::IndexLookup(BufferPool &pool, File &indexFile, std::vector<int> keys)
    : pool_(pool), indexFile_(indexFile), keys_(std::move(keys)), pos_(0), mode_(Mode::Keys)
{
}

IndexLookup IndexLookup::fromKey(BufferPool &pool, File &indexFile, int key)
{
    std::vector<int> keys;
    keys.push_back(key);
    return IndexLookup(pool, indexFile, std::move(keys));
}

IndexLookup IndexLookup::fromKeys(BufferPool &pool, File &indexFile, std::vector<int> keys)
{
    return IndexLookup(pool, indexFile, std::move(keys));
}

IndexLookup IndexLookup::fromKeyRange(BufferPool &pool, File &indexFile, int low_key, int high_key)
{
    LOG_INFO("Starting index scan for keys in range [{}, {}]", low_key, high_key);
    IndexLookup lookup(pool, indexFile, std::vector<int>{});
    lookup.mode_ = Mode::Range;

    if (low_key <= high_key)
    {
        lookup.low_key_ = low_key;
        lookup.high_key_ = high_key;
        lookup.current_key_ = low_key;
    }
    else
    {
        // 空レンジ: current を high+1 にして即座に EOF 相当にする
        lookup.low_key_ = low_key;
        lookup.high_key_ = high_key;
        lookup.current_key_ = high_key + 1;
    }
    LOG_INFO("Finished index scan for keys in range [{}, {}]", low_key, high_key);
    return lookup;
}

std::optional<RID> IndexLookup::next()
{
    if (mode_ == Mode::Keys)
    {
        while (pos_ < keys_.size())
        {
            int key = keys_[pos_++];
            auto location = BTreeCursor::findRecordLocation(pool_, indexFile_, key);
            if (!location.has_value())
            {
                continue;
            }

            return RID{location->first, location->second};
        }
    }
    else
    {
        while (current_key_ <= high_key_)
        {
            int key = current_key_++;
            auto location = BTreeCursor::findRecordLocation(pool_, indexFile_, key);
            if (!location.has_value())
            {
                continue;
            }

            return RID{location->first, location->second};
        }
    }

    return std::nullopt;
}