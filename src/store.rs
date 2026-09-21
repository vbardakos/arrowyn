use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, FromFn},
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
    ops::{Deref, Range},
    os::unix::fs::FileExt,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use bincode_next::{Decode, Encode, config, decode_from_slice};
use pyo3::marshal::VERSION;
use thiserror::Error;

const CURR_VERSION: u8 = 0;

pub type Offset = usize;
pub type RecordLen = usize;

#[derive(Debug)]
pub struct CaskStore {
    file: FileGuard,
    mmap: memmap2::Mmap,
    read_cache: HashMap<Vec<u8>, (Offset, RecordLen)>,
    purge_cache: HashSet<(Offset, RecordLen)>,
    uncommitted: u32,
}

impl CaskStore {
    const VERSION: Range<usize> = Range { start: 0, end: 1 };
    const RECLEN: Range<usize> = Range { start: 1, end: 5 };
    const MARKER: Range<usize> = Range { start: 5, end: 6 };
    const OFFSET: Range<usize> = Range { start: 6, end: 10 };
    const PRV_OFFSET: Range<usize> = Range { start: 10, end: 14 };
    const TIMESTAMP: Range<usize> = Range { start: 14, end: 18 };
    const FLAGS: Range<usize> = Range { start: 18, end: 20 };
    const KEYLEN: Range<usize> = Range { start: 20, end: 24 };

    const HEAD_LEN: usize = Self::FLAGS.end;

    pub fn try_new<P: AsRef<Path>>(pathlike: P) -> std::io::Result<Self> {
        let file: FileGuard = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(pathlike.as_ref())?
            .into();

        Ok(Self {
            file,
            mmap: unsafe { memmap2::Mmap::map(&file)? },
            read_cache: HashMap::new(),
            purge_cache: HashSet::new(),
            uncommitted: u32::MAX,
        })
    }

    pub fn get<D>(&self, key: &[u8]) -> Option<Result<D, CaskStoreError>>
    where
        D: Decode<()>,
    {
        let (offset, reclen) = self.find_offset(key)?;

        let prange = (Self::KEYLEN.end + key.len()..reclen).rshift(offset);
        let source = unsafe { self.mmap.get_unchecked(prange) };
        match decode_from_slice::<D>(source, config::standard()) {
            Ok((payload, _)) => Some(Ok(payload)),
            Err(e) => Some(e.map_err(CaskStoreError::DecodeError)),
        }
    }

    pub fn get_history<D>(&self, key: &[u8]) -> Box<dyn Iterator + '_>
    where
        D: Decode<()>,
    {
        if let Some((init, _)) = self.find_offset(key) {
            let keylen = key.len();
            unsafe { self.prev_recs(init) }
                .map(|offset| {
                    let reclen = unsafe {
                        self.mmap
                            .get_unchecked(Self::RECLEN.rshift(offset))
                            .get_u32(0..4)
                    };
                    let prange = (Self::KEYLEN.end + keylen..reclen).rshift(offset);
                    let source = unsafe { self.mmap.get_unchecked(prange) };

                    Box::new(
                        decode_from_slice::<D>(source, config::standard())
                            .map(|(p, _)| p)
                            .map_err(CaskStoreError::DecodeError),
                    )
                })
                .into()
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Upserts new record; the new record doesn't appear until commit
    pub fn upsert<Codec, Ctx>(&mut self, key: &[u8], payload: Codec)
    where
        Codec: Encode + Decode<Ctx>,
    {
        todo!()
    }

    pub fn commit(&mut self) {
        todo!()
    }

    /// Finds the offset of the record and return it
    fn find_offset(&mut self, key: &[u8]) -> Option<(Offset, RecordLen)> {
        use Marker::*;

        if let Some(&(offset, len)) = self.read_cache.get(key) {
            return Some((offset, len));
        }

        let buf: &[u8] = &self.mmap;
        let len: &[u8] = &(key.len() as u32).to_be_bytes();

        // new: reserved -> reserved -> uncalive -> uncalive -> alive
        // old: alive    -> uncsuper -> uncsuper -> supersed -> superseded
        let markers = &[Alive, UncommittedAlive, UncommittedSuperseded];

        let mut offset = 0usize;
        while Self::HEAD_LEN < buf.len() - offset {
            // SAFETY: reclen, version, and marker are checked in the loop condition
            let reclen = unsafe { buf.get_u32(Self::RECLEN.rshift(offset)) } as usize;

            // early exit for partial tail writes
            if reclen == 0 || reclen > buf.len() - offset {
                break;
            }

            unsafe {
                // SAFETY: all fields range bounds are checked
                let rec: &[u8] = buf.get_unchecked(offset..offset + reclen);
                if let Some(marker) = Self::match_rec(&rec, &key, len, markers) {
                    let mut marker = marker;
                    if marker == UncommittedAlive {
                        let offset = self.prev_recs(offset).next() else {
                            return None;
                        };
                    }

                    self.read_cache.insert(key.to_vec(), (offset, reclen));
                    return Some((offset, reclen));
                }
            }

            offset += reclen;
        }

        None
    }

    // SAFETY: assumes all necessary checks have already been done before the call
    unsafe fn match_rec(rec: &[u8], key: &[u8], len: &[u8], markers: &[Marker]) -> Option<Marker> {
        unsafe {
            if VERSION != rec.get_unchecked(Self::VERSION) {
                return None;
            }

            let mut marker = rec.get_marker(Self::MARKER);
            if markers.contains(&marker) {
                return None;
            }

            let actual_len: &[u8] = rec.get_unchecked(Self::KEYLEN);
            if actual_len != len {
                return None;
            }

            let curr_key: &[u8] = rec.get_unchecked(Self::KEYLEN.end..Self::KEYLEN.end + key.len());
            if curr_key != key {
                return None;
            }

            Some(marker)
        }
    }

    fn encode<Codec>(
        &self,
        key: &[u8],
        payload: Codec,
    ) -> Result<impl Fn(u32) -> Vec<u8>, CaskStoreError>
    where
        Codec: Encode + Decode<()>,
    {
        let conf = config::standard();
        let payload = bincode_next::encode_to_vec(payload, conf)?;
        let total_len = Self::HEAD_LEN + 4 + key.len() + payload.len();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;
        let mut rec: Vec<u8> = Vec::with_capacity(total_len);

        // [VERSION] [LENGTH] [MARKER]
        // [OFFSET] [PRV_OFFSET] [TIMESTAMP]
        // [FLAGS] [KEYLEN] [KEY] [PAYLOAD]
        rec.push(CURR_VERSION);
        rec.extend_from_slice(&total_len.to_be_bytes());
        rec.push(Marker::Reserved as u8);
        rec.extend_from_slice(&[0; 4]);
        rec.extend_from_slice(
            &self
                .find_offset(key)
                .map(|(o, _)| o as u32)
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        rec.extend_from_slice(&timestamp.to_be_bytes());
        rec.extend_from_slice(&[0; 2]);
        rec.extend_from_slice(&(key.len() as u32).to_be_bytes());
        rec.extend_from_slice(key);
        rec.extend_from_slice(payload.as_slice());

        let closure = move |offset: u32| {
            rec[Self::OFFSET] = offset.to_be_bytes();
            rec
        };
        Ok(closure)
    }

    unsafe fn swap_marker(&mut self, offset: usize, marker: Marker) -> Marker {
        let marker_offset = (offset + Self::MARKER.start) as u64;
        let mut old_marker = [0u8; 1];
        unsafe {
            // SAFETY: at this point it assumes the read is inbounds
            self.file
                .read_exact_at(&mut old_marker, marker_offset)
                .unwrap_unchecked();

            // SAFETY: doesn't need lock because single byte ops are atomic
            self.file
                .write_all_at(&[marker as u8], marker_offset)
                .unwrap_unchecked();

            // SAFETY: marker is expected to be correct at this point
            Marker::try_from(old_marker.get_unchecked(0)).unwrap_unchecked()
        }
    }

    unsafe fn prev_recs(&self, offset: usize) -> impl Iterator<Item = usize> {
        let mut offset = offset;
        let mut enc_offset: &[u8];
        std::iter::from_fn(move || unsafe {
            enc_offset = self.mmap.get_unchecked(Self::PRV_OFFSET.rshift(offset));
            offset = enc_offset.get_u32(0..4) as usize;
            if offset == u32::MAX {
                None
            } else {
                Some(offset)
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum Marker {
    // initial write state
    Reserved = 0,

    // consistency control states
    UncommittedAlive = 1,
    UncommittedSuperseded = 2,
    UncommittedPurged = 3,

    // committed states
    Alive = 4,
    Superseded = 5,
    Purged = 6,
}

impl TryFrom<u8> for Marker {
    type Error = CaskStoreError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        use CaskStoreError::Other;
        use Marker::*;

        match byte {
            0 => Ok(Reserved),
            1 => Ok(UncommittedAlive),
            2 => Ok(UncommittedSuperseded),
            3 => Ok(UncommittedPurged),
            4 => Ok(Alive),
            5 => Ok(Superseded),
            6 => Ok(Purged),
            b => Err(Other(format!("unexpected marker byte: {b}"))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FileGuard(File);

impl From<File> for FileGuard {
    fn from(value: File) -> Self {
        FileGuard(value)
    }
}

impl Deref for FileGuard {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        self.0.unlock().unwrap()
    }
}

pub trait RangeShiftExt {
    fn rshift(&self, value: usize) -> Self;
    fn lshift(&self, value: usize) -> Self;
}

impl RangeShiftExt for Range<usize> {
    fn rshift(&self, value: usize) -> Self {
        Range {
            start: self.start + value,
            end: self.end + value,
        }
    }

    fn lshift(&self, value: usize) -> Self {
        Range {
            start: self.start - value,
            end: self.end - value,
        }
    }
}

trait DecodeExt {
    unsafe fn get_u32(&self, r: Range<usize>) -> u32;
    unsafe fn get_marker(&self, r: Range<usize>) -> Marker;
}

impl<'a> DecodeExt for &'a [u8] {
    unsafe fn get_u32(&self, r: Range<usize>) -> u32 {
        let bytes: [u8; 4] = unsafe { self.get_unchecked(r).try_into().unwrap_unchecked() };
        u32::from_be_bytes(bytes)
    }

    unsafe fn get_marker(&self, r: Range<usize>) -> Marker {
        unsafe { Marker::try_from(self.get_unchecked(r)).unwrap_unchecked() }
    }
}

#[derive(Debug, Error)]
pub enum CaskStoreError {
    #[error("Failed to encode payload: {0}")]
    EncodeError(#[from] bincode_next::error::EncodeError),
    #[error("Failed to decode payload: {0}")]
    DecodeError(#[from] bincode_next::error::DecodeError),
    #[error("Failed to build record timestamp: {0}")]
    TimestampError(#[from] std::time::SystemTimeError),
    #[error("{0}")]
    Other(String),
}
