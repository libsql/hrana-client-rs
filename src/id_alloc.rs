use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct IdAllocator {
    used_ids: HashSet<i32>,
    free_ids: HashSet<i32>,
}

impl IdAllocator {
    pub(crate) fn allocate(&mut self) -> i32 {
        if let Some(id) = self.free_ids.iter().next().copied() {
            self.free_ids.remove(&id);
            self.used_ids.insert(id);

            if !self.used_ids.contains(&(self.used_ids.len() as i32 - 1)) {
                self.free_ids.insert(self.used_ids.len() as i32 - 1);
            }

            return id;
        }

        let id = self.used_ids.len() as i32;
        self.used_ids.insert(id);
        id
    }

    pub(crate) fn free(&mut self, id: i32) {
        if !self.used_ids.remove(&id) {
            panic!("freeing an id that is not allocated");
        }

        if id < self.used_ids.len() as i32 {
            self.free_ids.insert(id);
        }
    }
}
