use std::fmt::{self, Debug};

use super::memcmpable;

pub fn encode(elems: impl Iterator<Item = impl AsRef<[u8]>>, bytes: &mut Vec<u8>) {
    elems.for_each(|elem| {
        let elem_bytes = elem.as_ref();
        let len = memcmpable::encoded_size(elem_bytes.len());
        bytes.reserve(len);
        memcmpable::encode(elem_bytes, bytes);
    });
}

pub fn decode(bytes: &[u8], elems: &mut Vec<Vec<u8>>) {
    let mut rest = bytes;
    while !rest.is_empty() {
        let mut elem = vec![];
        memcmpable::decode(&mut rest, &mut elem);
        elems.push(elem);
    }
}

pub struct Pretty<'a, T>(pub &'a [T]);

impl<'a, T: AsRef<[u8]>> Debug for Pretty<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_tuple("Tuple");
        for elem in self.0 {
            let bytes = elem.as_ref();
            match std::str::from_utf8(bytes) {
                Ok(s) => {
                    d.field(&format_args!("{:?} {:02x?}", s, bytes));
                }
                Err(_) => {
                    d.field(&format_args!("{:02x?}", bytes));
                }
            }
        }
        d.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_test() {
        let mut enc1 = vec![];
        let org1: Vec<&[u8]> = vec![b"hello", b",", b"world", b"!"];
        encode(org1.iter(), &mut enc1);
        let expected = [
            b'h', b'e', b'l', b'l', b'o', 0u8, 0u8, 0u8, 5u8, b',', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            0u8, 1u8, b'w', b'o', b'r', b'l', b'd', 0u8, 0u8, 0u8, 5u8, b'!', 0u8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 1u8,
        ];
        assert_eq!(enc1.as_slice(), expected);
    }

    #[test]
    fn decode_test() {
        let mut dec1 = vec![];
        let org1 = &[
            b'h', b'e', b'l', b'l', b'o', 0u8, 0u8, 0u8, 5u8, b',', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            0u8, 1u8, b'w', b'o', b'r', b'l', b'd', 0u8, 0u8, 0u8, 5u8, b'!', 0u8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 1u8,
        ];
        decode(org1, &mut dec1);
        let expected: &[&[u8]] = &[b"hello", b",", b"world", b"!"];
        assert_eq!(dec1.as_slice(), expected);
    }

    #[test]
    fn fmt_for_pretty_test() {
        let mut enc1 = vec![];
        let org1: Vec<&[u8]> = vec![b"hello", b",", b"world", b"!"];
        encode(org1.iter(), &mut enc1);

        let mut dec1 = vec![];
        decode(&enc1, &mut dec1);

        assert_eq!(
	    format!("{:?}", Pretty(&dec1)),
	    "Tuple(\"hello\" [68, 65, 6c, 6c, 6f], \",\" [2c], \"world\" [77, 6f, 72, 6c, 64], \"!\" [21])",
	);
    }
}
