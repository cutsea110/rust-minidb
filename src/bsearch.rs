use std::cmp::Ordering::{self, Greater, Less};

pub fn binary_search_by<F>(mut size: usize, mut f: F) -> Result<usize, usize>
where
    F: FnMut(usize) -> Ordering,
{
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;
        let cmp = f(mid);
        if cmp == Less {
            left = mid + 1;
        } else if cmp == Greater {
            right = mid;
        } else {
            return Ok(mid);
        }
        size = right - left;
    }
    Err(left)
}

#[cfg(test)]
mod tests {
    use super::binary_search_by;

    #[test]
    fn test() {
        let a = vec![1, 2, 3, 5, 8, 13, 21];
        assert_eq!(Ok(0), binary_search_by(a.len(), |idx| a[idx].cmp(&1)));
        assert_eq!(Err(0), binary_search_by(a.len(), |idx| a[idx].cmp(&0)));
        assert_eq!(Ok(1), binary_search_by(a.len(), |idx| a[idx].cmp(&2)));
        assert_eq!(Ok(4), binary_search_by(a.len(), |idx| a[idx].cmp(&8)));
        assert_eq!(Err(4), binary_search_by(a.len(), |idx| a[idx].cmp(&6)));
        assert_eq!(Ok(6), binary_search_by(a.len(), |idx| a[idx].cmp(&21)));
        assert_eq!(Err(7), binary_search_by(a.len(), |idx| a[idx].cmp(&22)));
    }
}
