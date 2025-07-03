pub(crate) struct IterRevolver<'a, T> {
    shards: *mut T,
    next: usize,
    len: usize,
    phantom: std::marker::PhantomData<&'a mut [T]>,
}

impl<'a, T> From<&'a mut [T]> for crate::chunked::IterRevolver<'a, T> {
    fn from(shards: &'a mut [T]) -> crate::chunked::IterRevolver<'a, T> {
        IterRevolver {
            next: 0,
            len: shards.len(),
            shards: shards.as_mut_ptr(),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T> Iterator for IterRevolver<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next < self.len {
            self.next += 1;
        } else {
            self.next = 1;
        }
        unsafe { Some(&mut *self.shards.offset(self.next as isize - 1)) }
    }
}