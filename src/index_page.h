#pragma once

#include "page.h"
#include "leaf_cell.h"
#include "intermediate_cell.h"
#include <optional>
#include <utility>

// LeafIndexPage and InternalIndexPage provide typed views over a generic Page
// for B+tree index nodes. They assume the underlying Page buffer/layout is
// already initialized and do not own the storage.

class LeafIndexPage {
public:
    explicit LeafIndexPage(Page &page) : page_(page) {
        if (!page_.isLeaf()) {
            throw std::logic_error("LeafIndexPage constructed with non-leaf Page");
        }
    }

    Page &page() { return page_; }
    const Page &page() const { return page_; }

    bool hasKey(int key) const;
    std::optional<std::pair<uint16_t, uint16_t>> findRef(int key, bool do_invalidate);

    void transferAndCompactTo(LeafIndexPage &dst, char *separate_key);

private:
    Page &page_;
};

class InternalIndexPage {
public:
    explicit InternalIndexPage(Page &page) : page_(page) {
        if (page_.isLeaf()) {
            throw std::logic_error("InternalIndexPage constructed with leaf Page");
        }
    }

    Page &page() { return page_; }
    const Page &page() const { return page_; }

    uint16_t findChildPage(int key);
    void transferAndCompactTo(InternalIndexPage &dst, char *separate_key);

private:
    Page &page_;
};
