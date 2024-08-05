use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use crate::io::FileIO;
use crate::spec::Snapshot;

#[derive(Debug, Clone)]
pub struct SnapshotManager {
    file_io: FileIO,
    table_path: String,
    branch: String,
}

impl SnapshotManager {
    const DEFAULT_MAIN_BRANCH: &'static str = "main";
    const BRANCH_PREFIX: &'static str = "branch-";

    pub fn new(file_io: FileIO, table_path: String, branch: Option<String>) -> Self {
        Self {
            file_io,
            table_path,
            branch: branch.unwrap_or_else(|| SnapshotManager::DEFAULT_MAIN_BRANCH.to_string()),
        }
    }

    pub fn copy_with_branch(&self, branch_name: String) -> Self {
        Self {
            file_io: self.file_io.clone(),
            table_path: self.table_path.clone(),
            branch: branch_name,
        }
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn snapshot_path(&self, snapshot_id: i64) -> String {
        format!("{}/snapshot-{}", self.snapshot_directory(), snapshot_id)
    }

    pub fn snapshot_directory(&self) -> String {
        format!("{}/snapshot", self.branch_path(&self.table_path, &self.branch));
    }

    pub fn snapshot(&self, snapshot_id: i64) -> Snapshot {
        let path = self.snapshot_path(snapshot_id);
        Snapshot::from_path(&self.file_io, &path)
    }

    pub fn snapshot_exists(&self, snapshot_id: i64) -> bool {
        let path = self.snapshot_path(snapshot_id);
        self.file_io.exists(&path)
    }

    pub fn latest_snapshot(&self) -> Option<Snapshot> {
        self.latest_snapshot_id().map(|id| self.snapshot(id))
    }

    pub fn latest_snapshot_id(&self) -> Option<i64> {
        self.find_latest(&self.snapshot_directory(), "snapshot-")
    }

    pub fn earliest_snapshot(&self) -> Option<Snapshot> {
        self.earliest_snapshot_id().map(|id| self.snapshot(id))
    }

    pub fn earliest_snapshot_id(&self) -> Option<i64> {
        self.find_earliest(&self.snapshot_directory(), "snapshot-")
    }

    pub fn pick_or_latest<F>(&self, predicate: F) -> Option<i64>
    where
        F: Fn(&Snapshot) -> bool,
    {
        let latest_id = self.latest_snapshot_id()?;
        let earliest_id = self.earliest_snapshot_id()?;
        for id in (earliest_id..=latest_id).rev() {
            if self.snapshot_exists(id) {
                let snapshot = self.snapshot(id);
                if predicate(&snapshot) {
                    return Some(snapshot.id());
                }
            }
        }
        Some(latest_id)
    }

    pub fn earlier_than_time_mills(&self, timestamp_mills: u64, start_from_changelog: bool) -> Option<u64> {
        let earliest_snapshot = self.earliest_snapshot_id()?;
        let earliest = if start_from_changelog {
            self.earliest_long_lived_changelog_id().unwrap_or(earliest_snapshot)
        } else {
            earliest_snapshot
        };
        let latest = self.latest_snapshot_id()?;
        if self.changelog_or_snapshot(earliest).time_millis() >= timestamp_mills {
            return Some(earliest - 1);
        }
        let mut low = earliest;
        let mut high = latest;
        while low < high {
            let mid = (low + high + 1) / 2;
            if self.changelog_or_snapshot(mid).time_millis() < timestamp_mills {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Some(low)
    }

    pub fn earlier_or_equal_time_mills(&self, timestamp_mills: u64) -> Option<Snapshot> {
        let earliest = self.earliest_snapshot_id()?;
        let latest = self.latest_snapshot_id()?;
        if self.snapshot(earliest).time_millis() > timestamp_mills {
            return None;
        }
        let mut low = earliest;
        let mut high = latest;
        let mut final_snapshot = None;
        while low <= high {
            let mid = low + (high - low) / 2;
            let snapshot = self.snapshot(mid);
            let commit_time = snapshot.time_millis();
            if commit_time > timestamp_mills {
                high = mid - 1;
            } else if commit_time < timestamp_mills {
                low = mid + 1;
                final_snapshot = Some(snapshot);
            } else {
                final_snapshot = Some(snapshot);
                break;
            }
        }
        final_snapshot
    }

    pub fn later_or_equal_watermark(&self, watermark: u64) -> Option<Snapshot> {
        let earliest = self.earliest_snapshot_id()?;
        let latest = self.latest_snapshot_id()?;
        if self.snapshot(latest).watermark() == u64::MIN {
            return None;
        }
        let mut earliest_watermark = self.snapshot(earliest).watermark();
        if earliest_watermark == u64::MIN {
            for id in earliest..=latest {
                earliest_watermark = self.snapshot(id).watermark();
                if earliest_watermark != u64::MIN {
                    break;
                }
            }
        }
        if earliest_watermark >= watermark {
            return Some(self.snapshot(earliest));
        }
        let mut low = earliest;
        let mut high = latest;
        let mut final_snapshot = None;
        while low <= high {
            let mid = low + (high - low) / 2;
            let snapshot = self.snapshot(mid);
            let commit_watermark = snapshot.watermark();
            if commit_watermark == u64::MIN {
                for id in (low..=mid).rev() {
                    let snapshot = self.snapshot(id);
                    let commit_watermark = snapshot.watermark();
                    if commit_watermark != u64::MIN {
                        break;
                    }
                }
            }
            if commit_watermark > watermark {
                high = mid - 1;
                final_snapshot = Some(snapshot);
            } else if commit_watermark < watermark {
                low = mid + 1;
            } else {
                final_snapshot = Some(snapshot);
                break;
            }
        }
        final_snapshot
    }

    pub fn snapshot_count(&self) -> io::Result<u64> {
        Ok(fs::read_dir(self.snapshot_directory())?.count() as u64)
    }

    pub fn snapshots(&self) -> io::Result<Vec<Snapshot>> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(self.snapshot_directory())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.file_name().unwrap().to_str().unwrap().starts_with("snapshot-") {
                snapshots.push(Snapshot::from_path(&self.file_io, &path));
            }
        }
        snapshots.sort_by_key(|s| s.id());
        Ok(snapshots)
    }

    pub fn snapshots_within_range(&self, max_snapshot_id: Option<u64>, min_snapshot_id: Option<u64>) -> io::Result<Vec<Snapshot>> {
        let mut snapshots = Vec::new();
        let lower_bound = min_snapshot_id.unwrap_or_else(|| self.earliest_snapshot_id().unwrap());
        let upper_bound = max_snapshot_id.unwrap_or_else(|| self.latest_snapshot_id().unwrap());
        for id in lower_bound..=upper_bound {
            snapshots.push(self.snapshot(id));
        }
        snapshots.sort_by_key(|s| s.id());
        Ok(snapshots)
    }

    pub fn safely_get_all_snapshots(&self) -> io::Result<Vec<Snapshot>> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(self.snapshot_directory())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.file_name().unwrap().to_str().unwrap().starts_with("snapshot-") {
                if let Ok(snapshot) = Snapshot::safely_from_path(&self.file_io, &path) {
                    snapshots.push(snapshot);
                }
            }
        }
        snapshots.sort_by_key(|s| s.id());
        Ok(snapshots)
    }

    pub fn try_get_non_snapshot_files<F>(&self, file_status_filter: F) -> io::Result<Vec<PathBuf>>
    where
        F: Fn(&fs::Metadata) -> bool,
    {
        self.list_path_with_filter(&self.snapshot_directory(), file_status_filter, |path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            !name.starts_with("snapshot-") && name != "EARLIEST" && name != "LATEST"
        })
    }

    fn list_path_with_filter<F, G>(&self, directory: &Path, file_status_filter: F, file_filter: G) -> io::Result<Vec<PathBuf>>
    where
        F: Fn(&fs::Metadata) -> bool,
        G: Fn(&Path) -> bool,
    {
        let mut paths = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if file_status_filter(&entry.metadata()?) && file_filter(&path) {
                paths.push(path);
            }
        }
        Ok(paths)
    }

    pub fn latest_snapshot_of_user(&self, user: &str) -> Option<Snapshot> {
        let latest_id = self.latest_snapshot_id()?;
        let earliest_id = self.earliest_snapshot_id()?;
        for id in (earliest_id..=latest_id).rev() {
            let snapshot = self.snapshot(id);
            if snapshot.commit_user() == user {
                return Some(snapshot);
            }
        }
        None
    }

    pub fn find_snapshots_for_identifiers(&self, user: &str, identifiers: &[u64]) -> Vec<Snapshot> {
        if identifiers.is_empty() {
            return Vec::new();
        }
        let latest_id = self.latest_snapshot_id()?;
        let earliest_id = self.earliest_snapshot_id()?;
        let min_searched_identifier = *identifiers.iter().min().unwrap();
        let mut matched_snapshots = Vec::new();
        let mut remaining_identifiers: HashSet<u64> = identifiers.iter().cloned().collect();
        for id in (earliest_id..=latest_id).rev() {
            let snapshot = self.snapshot(id);
            if snapshot.commit_user() == user && remaining_identifiers.remove(&snapshot.commit_identifier()) {
                matched_snapshots.push(snapshot);
                if snapshot.commit_identifier() <= min_searched_identifier {
                    break;
                }
            }
        }
        matched_snapshots
    }

    pub fn traversal_snapshots_from_latest_safely<F>(&self, checker: F) -> Option<Snapshot>
    where
        F: Fn(&Snapshot) -> bool,
    {
        let latest_id = self.latest_snapshot_id()?;
        let earliest_id = self.earliest_snapshot_id()?;
        for id in (earliest_id..=latest_id).rev() {
            let snapshot = self.snapshot(id);
            if checker(&snapshot) {
                return Some(snapshot);
            }
        }
        None
    }

    fn find_latest(&self, dir: &str, prefix: &str) -> Option<i64> {
        self.read_hint("LATEST", dir).or_else(|| self.find_by_list_files(u64::max, dir, prefix))
    }

    fn find_earliest(&self, dir: &str, prefix: &str) -> Option<i64> {
        self.read_hint("EARLIEST", dir).or_else(|| self.find_by_list_files(u64::min, dir, prefix))
    }

    fn read_hint(&self, file_name: &str, dir: &str) -> Option<i64> {
        let path = dir.join(file_name);
        for _ in 0..3 {
            if let Ok(mut file) = fs::File::open(&path) {
                let mut content = String::new();
                if file.read_to_string(&mut content).is_ok() {
                    if let Ok(hint) = content.trim().parse() {
                        return Some(hint);
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        None
    }

    fn find_by_list_files<F>(&self, reducer: F, dir: &Path, prefix: &str) -> Option<u64>
    where
        F: Fn(u64, u64) -> u64,
    {
        let mut ids = Vec::new();
        for entry in fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.starts_with(prefix) {
                        if let Ok(id) = file_name[prefix.len()..].parse() {
                            ids.push(id);
                        }
                    }
                }
            }
        }
        ids.into_iter().reduce(reducer)
    }

    pub fn delete_latest_hint(&self) -> io::Result<()> {
        let path = format!("{}/LATEST", self.snapshot_directory());
        self.file_io.delete_file(path.as_str())
    }

    pub fn commit_latest_hint(&self, snapshot_id: u64) -> io::Result<()> {
        self.commit_hint(snapshot_id, "LATEST", &self.snapshot_directory())
    }

    pub fn commit_earliest_hint(&self, snapshot_id: u64) -> io::Result<()> {
        self.commit_hint(snapshot_id, "EARLIEST", &self.snapshot_directory())
    }

    fn commit_hint(&self, snapshot_id: u64, file_name: &str, dir: &str) -> io::Result<()> {
        let path = format!("{}/{}", dir, file_name);
        // todo replace with overwrite
        self.file_io.delete_file(path.as_str())?;
        let output = self.file_io.new_output(path.as_str());
        todo!()
        // file.write_all(snapshot_id.to_string().as_bytes())
    }

    /// todo move to branch manager

    pub fn is_main_branch(branch: &str) -> bool {
        branch == Self::DEFAULT_MAIN_BRANCH
    }
    pub fn branch_path(table_path: &str, branch: &str) -> String {
        if Self::is_main_branch(branch) {
            table_path.to_string()
        } else {
            format!("{}/branch/{}{}", table_path, Self::BRANCH_PREFIX, branch)
        }
    }
}
