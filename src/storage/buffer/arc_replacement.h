#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <list>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include "page_key.h"

class ARCReplacement {

 private:
    int t1_target_ = 0;
    int total_frame_count_;

    enum class ArcList {
        T1,
        T2,
        B1,
        B2,
    };

    struct ArcEntry {
        ArcList list;
        std::list<PageKey>::iterator it;
        std::optional<int> frame_id;
    };
    
    std::list<PageKey> t1_;
    std::list<PageKey> t2_;
    std::list<PageKey> b1_;
    std::list<PageKey> b2_;
    std::unordered_map<PageKey, ArcEntry, PageKeyHash> entries_;

    size_t residentSize() const {
        return t1_.size() + t2_.size();
    }

    void moveEntry(ArcEntry& entry, std::list<PageKey>& from,
                   std::list<PageKey>& to, ArcList destination) {
        // std::list::splice preserves the iterator, so entry.it remains valid.
        to.splice(to.end(), from, entry.it);
        entry.list = destination;
    }

    void removeLru(std::list<PageKey>& list) {
        const PageKey key = list.front();
        list.pop_front();
        entries_.erase(key);
    }

    void trimGhostListForNewPage() {
        const size_t capacity = static_cast<size_t>(total_frame_count_);
        const size_t recent_size = t1_.size() + b1_.size();
        const size_t total_size =
            recent_size + t2_.size() + b2_.size();

        if (recent_size == capacity) {
            // |T1| + |B1| <= total_frame_count_
            // make sure we don't cache old old data on B1.
            if (!b1_.empty()) {
                removeLru(b1_);
            }
        } else if (recent_size < capacity &&
                   total_size >= 2 * capacity) {
            // |T1| + |T2| + |B1| + |B2| <= 2*total_frame_count_
            if (!b2_.empty()) {
                removeLru(b2_);
            }
        }
    }

    std::optional<int> findEvictable(
        const std::list<PageKey>& list,
        const std::function<bool(int)>& is_evictable) const {
        for (const PageKey& key : list) {
            const ArcEntry& entry = entries_.at(key);
            if (entry.frame_id.has_value() &&
                is_evictable(*entry.frame_id)) {
                return entry.frame_id;
            }
        }
        return std::nullopt;
    }

  public:

    explicit ARCReplacement(int total_frame_count)
        : total_frame_count_(total_frame_count) {
        if (total_frame_count_ <= 0) {
            throw std::invalid_argument(
                "ARCReplacement requires a positive frame count.");
        }
    }

    void onEviction(int victim_frame_id, const PageKey& key){
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if (!entry.frame_id.has_value() ||
                *entry.frame_id != victim_frame_id) {
                throw std::logic_error("frame ID does not match ARC entry.");
            }
            if(entry.list == ArcList::T1){
                moveEntry(entry, t1_, b1_, ArcList::B1);
                entry.frame_id = std::nullopt;
                return;
            }else if(entry.list == ArcList::T2){
                moveEntry(entry, t2_, b2_, ArcList::B2);
                entry.frame_id = std::nullopt;
                return;
            }
        }
        throw std::logic_error("should not be called.");
    }

    void onResidentPageAccess(const PageKey& key){
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if(entry.list == ArcList::T1){
                moveEntry(entry, t1_, t2_, ArcList::T2);
                return;
            }else if(entry.list == ArcList::T2){
                moveEntry(entry, t2_, t2_, ArcList::T2);
                return;
            }
        }
        throw std::logic_error("This method should not be called for none-resident pages.");
    }

    /**
     * Call this only when there is no free frame.
     */
    std::optional<int> selectVictimFrame(
        const PageKey& key,
        const std::function<bool(int)>& is_evictable) const {
        auto it = entries_.find(key);
        if (it != entries_.end() &&
            (it->second.list == ArcList::T1 ||
             it->second.list == ArcList::T2)) {
            throw std::logic_error(
                "This method should not be called for resident pages.");
        }

        const bool b2_hit =
            it != entries_.end() && it->second.list == ArcList::B2;
        const bool prefer_t1 =
            !t1_.empty() &&
            (t1_.size() > static_cast<size_t>(t1_target_) ||
             (b2_hit &&
              t1_.size() == static_cast<size_t>(t1_target_)));

        if (prefer_t1) {
            return findEvictable(t1_, is_evictable);
        }

        return findEvictable(t2_, is_evictable);
    }

    /**
     * A free frame should exist before calling this method.
     */
    void onNoneResidentPageAccess(int frame_id, const PageKey& key){
        if (residentSize() >= static_cast<size_t>(total_frame_count_)) {
            throw std::logic_error(
                "onNoneResidentPageAccess requires a free frame.");
        }

        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            // A hit in B1 indicates that recency should be favored. If |B1| is small relative to |B2|, then the recency component is demonstrating strong effectiveness despite its limited allocation.
            // Consequently, ARC increases p by a larger amount, shifting cache capacity from T2 toward T1.
            if(entry.list == ArcList::B1){
                const int adjustment = std::max(
                    1, static_cast<int>(b2_.size() / b1_.size()));
                t1_target_ = std::min(
                    total_frame_count_, t1_target_ + adjustment);
                moveEntry(entry, b1_, t2_, ArcList::T2);
                entry.frame_id = frame_id;
                return;
            }else if(entry.list == ArcList::B2){
                const int adjustment = std::max(
                    1, static_cast<int>(b1_.size() / b2_.size()));
                t1_target_ = std::max(0, t1_target_ - adjustment);
                moveEntry(entry, b2_, t2_, ArcList::T2);
                entry.frame_id = frame_id;
                return;
            }
            throw std::logic_error(
                "This method should not be called for resident pages.");
        }else{
            trimGhostListForNewPage();
            // add page to T1.
            t1_.push_back(key);
            ArcEntry entry;
            entry.list = ArcList::T1;
            entry.it = std::prev(t1_.end());
            entry.frame_id = frame_id;
            entries_[key] = entry;
        }
    }
};
