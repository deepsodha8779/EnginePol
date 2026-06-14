/// Validates a ULID (Crockford Base32, 26 chars, case-insensitive)
pub fn is_valid_ulid(value: &str) -> bool {
    if value.len() != 26 {
        return false;
    }

    value.chars().all(|c| match c {
        '0'..='9' => true,
        'A'..='Z' => matches!(
            c,
            'A' | 'B'
                | 'C'
                | 'D'
                | 'E'
                | 'F'
                | 'G'
                | 'H'
                | 'J'
                | 'K'
                | 'M'
                | 'N'
                | 'P'
                | 'Q'
                | 'R'
                | 'S'
                | 'T'
                | 'V'
                | 'W'
                | 'X'
                | 'Y'
                | 'Z'
        ),
        'a'..='z' => matches!(
            c,
            'a' | 'b'
                | 'c'
                | 'd'
                | 'e'
                | 'f'
                | 'g'
                | 'h'
                | 'j'
                | 'k'
                | 'm'
                | 'n'
                | 'p'
                | 'q'
                | 'r'
                | 's'
                | 't'
                | 'v'
                | 'w'
                | 'x'
                | 'y'
                | 'z'
        ),
        _ => false,
    })
}
