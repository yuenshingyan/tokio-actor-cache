use crc16_xmodem_fast::hash;

pub fn hash_id(val: &str, num_shards: u16) -> u16 {
    // Step 1: Hash the ISIN to CRC16 XMODEM (returns u16)
    let crc = hash(val.as_bytes());

    // Step 2: CRC16 is already u16 (0..=65535), but output as hex for clarity
    let hex = format!("{:04X}", crc);
    // Convert hex string to decimal (redundant, since u16, but as per instructions)
    let decimal = u16::from_str_radix(&hex, 16).expect("Invalid hex");

    // Step 3: Compute shard ID
    decimal % num_shards
}
