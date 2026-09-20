use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs::{File, OpenOptions},
    ops::{Deref, Range},
    path::Path,
};

#[derive(Debug)]
pub struct CaskStore {
    file: FileGuard,
    mmap: memmap2::Mmap,
    read_cache: HashMap<Vec<u8>, usize>,
    purge_cache: HashSet<usize>,
}

impl CaskStore {
    const VERSION: Range<usize> = Range { start: 0, end: 1 };
    const LENGTH: Range<usize> = Range { start: 1, end: 5 };
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
        })
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
