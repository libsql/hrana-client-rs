use std::collections::HashSet;

/// An allocator of non-negative integer ids.
///
/// This clever data structure has these "ideal" properties:
/// - It consumes memory proportional to the number of used ids (which is optimal).
/// - All operations are O(1) time.
/// - The allocated ids are small (with a slight modification, we could always provide the smallest possible
/// id).
#[derive(Default)]
pub(crate) struct IdAllocator {
    /// Set of all allocated ids
    used_ids: HashSet<i32>,
    /// Set of all free ids lower than `#usedIds.size`
    free_ids: HashSet<i32>,
}

impl IdAllocator {
    /// Returns an id that was free, and marks it as used.
    pub(crate) fn allocate(&mut self) -> i32 {
        if let Some(id) = self.free_ids.iter().next().copied() {
            self.free_ids.remove(&id);
            self.used_ids.insert(id);

            // maintain the invariant of `#freeIds`
            if !self.used_ids.contains(&(self.used_ids.len() as i32 - 1)) {
                self.free_ids.insert(self.used_ids.len() as i32 - 1);
            }

            return id;
        }

        let id = self.used_ids.len() as i32;
        self.used_ids.insert(id);
        id
    }

    /// the `#freeIds` set is empty, so there are no free ids lower than `#usedIds.size`
    /// this means that `#usedIds` is a set that contains all numbers from 0 to `#usedIds.size - 1`,
    /// so `#usedIds.size` is free
    pub(crate) fn free(&mut self, id: i32) {
        if !self.used_ids.remove(&id) {
            panic!("freeing an id that is not allocated");
        }

        // maintain the invariant of `#freeIds`
        if id < self.used_ids.len() as i32 {
            self.free_ids.insert(id);
        }
    }
}
