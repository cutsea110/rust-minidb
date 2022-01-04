use std::cmp;

const ESCAPE_LENGTH: usize = 9;

pub fn encoded_size(len: usize) -> usize {
    (len + (ESCAPE_LENGTH - 1)) / (ESCAPE_LENGTH - 1) * ESCAPE_LENGTH
}

pub fn encode(mut src: &[u8], dst: &mut Vec<u8>) {
    loop {
        let copy_len = cmp::min(ESCAPE_LENGTH - 1, src.len());
        dst.extend(&src[0..copy_len]);
        src = &src[copy_len..];
        if src.is_empty() {
            let pad_size = ESCAPE_LENGTH - 1 - copy_len;
            if pad_size > 0 {
                dst.resize(dst.len() + pad_size, 0);
            }
            dst.push(copy_len as u8);
            break;
        }
        dst.push(ESCAPE_LENGTH as u8);
    }
}

pub fn decode(src: &mut &[u8], dst: &mut Vec<u8>) {
    loop {
        let extra = src[ESCAPE_LENGTH - 1];
        let len = cmp::min(ESCAPE_LENGTH - 1, extra as usize);
        dst.extend_from_slice(&src[..len]);
        *src = &src[ESCAPE_LENGTH..];
        if extra < ESCAPE_LENGTH as u8 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_size_test() {
        assert_eq!(encoded_size(0), ESCAPE_LENGTH);
        assert_eq!(encoded_size(1), ESCAPE_LENGTH);
        assert_eq!(encoded_size(8), 2 * ESCAPE_LENGTH);
        assert_eq!(encoded_size(9), 2 * ESCAPE_LENGTH);
        assert_eq!(encoded_size(15), 2 * ESCAPE_LENGTH);
        assert_eq!(encoded_size(16), 3 * ESCAPE_LENGTH);
    }

    #[test]
    fn encode_test() {
        let org1 = b"";
        let mut enc1 = vec![];
        encode(org1, &mut enc1);
        assert_eq!(enc1.len(), 9);
        assert_eq!(&enc1[..], &[b'\0', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]); // 合ってる?

        let org2 = b"1";
        let mut enc2 = vec![];
        encode(org2, &mut enc2);
        assert_eq!(enc2.len(), 9);
        assert_eq!(&enc2[..], &[b'1', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8]);

        let org3 = b"12345678";
        let mut enc3 = vec![];
        encode(org3, &mut enc3);
        assert_eq!(enc3.len(), 9);
        assert_eq!(
            &enc3[..],
            &[b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 8u8]
        );

        let org3 = b"123456789";
        let mut enc3 = vec![];
        encode(org3, &mut enc3);
        assert_eq!(enc3.len(), 18);

        let org4 = b"123456789";
        let mut enc4 = vec![];
        encode(org4, &mut enc4);
        assert_eq!(enc4.len(), 18);
        assert_eq!(
            &enc4[..],
            &[
                b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9u8, b'9', 0u8, 0u8, 0u8, 0u8, 0u8,
                0u8, 0u8, 1u8
            ]
        );

        let org5 = b"1234567890abcdef";
        let mut enc5 = vec![];
        encode(org5, &mut enc5);
        assert_eq!(enc5.len(), 18);
        assert_eq!(
            &enc5[..],
            &[
                b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9u8, b'9', b'0', b'a', b'b', b'c',
                b'd', b'e', b'f', 8u8
            ]
        );

        let org6 = b"1234567890abcdefg";
        let mut enc6 = vec![];
        encode(org6, &mut enc6);
        assert_eq!(enc6.len(), 27);
        assert_eq!(
            &enc6[..],
            &[
                b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9u8, b'9', b'0', b'a', b'b', b'c',
                b'd', b'e', b'f', 9u8, b'g', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8
            ]
        );
    }

    #[test]
    fn decode_test() {
        let mut enc1: &[u8] = &[b'\0', 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut dec1 = vec![];
        decode(&mut enc1, &mut dec1);
        assert_eq!(dec1.as_slice(), b"");
    }

    #[test]
    fn test() {
        let org1 = b"helloworld!memcmpable";
        let org2 = b"foobarbazhogehuga";
        let org3 = b"charlen8";

        let mut enc = vec![];
        encode(org1, &mut enc);
        encode(org2, &mut enc);
        encode(org3, &mut enc);

        let mut rest = &enc[..];

        let mut dec1 = vec![];
        decode(&mut rest, &mut dec1);
        assert_eq!(org1, dec1.as_slice());
        let mut dec2 = vec![];
        decode(&mut rest, &mut dec2);
        assert_eq!(org2, dec2.as_slice());
        let mut dec3 = vec![];
        decode(&mut rest, &mut dec3);
        assert_eq!(org3, dec3.as_slice());
    }
}
