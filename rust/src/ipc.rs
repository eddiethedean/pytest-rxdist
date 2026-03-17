use serde_json::Value;
use std::io::{Read, Write};

pub fn read_exact_or_eof(mut r: impl Read, n: usize) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = vec![0u8; n];
    let mut read_total = 0usize;
    while read_total < n {
        let m = r.read(&mut buf[read_total..])?;
        if m == 0 {
            if read_total == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
        }
        read_total += m;
    }
    Ok(Some(buf))
}

pub fn read_frame(mut r: impl Read) -> std::io::Result<Option<(String, Value)>> {
    let hdr = match read_exact_or_eof(&mut r, 4)? {
        None => return Ok(None),
        Some(h) => h,
    };
    let size = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let data = read_exact_or_eof(&mut r, size)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"))?;
    let obj: Value = rmp_serde::from_slice(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("msgpack decode: {e}")))?;
    let msg_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let payload = obj.get("payload").cloned().unwrap_or_else(|| serde_json::json!({}));
    Ok(Some((msg_type, payload)))
}

pub fn write_frame(mut w: impl Write, msg_type: &str, payload: &Value) -> std::io::Result<()> {
    let frame = serde_json::json!({"type": msg_type, "payload": payload});
    let data = rmp_serde::to_vec(&frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("msgpack encode: {e}")))?;
    let size = (data.len() as u32).to_be_bytes();
    w.write_all(&size)?;
    w.write_all(&data)?;
    w.flush()?;
    Ok(())
}

