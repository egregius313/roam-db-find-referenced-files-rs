use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};

use log::{debug, info};
use rusqlite::{Connection, Statement};

use org_roam_db::RoamFile;

/// Find all file names of notes referenced by a given file in the org-roam database.
///
/// # Arguments
/// * `stmt` - A prepared statement to find referenced files.
/// * `path` - The file to find references for.
///
/// # Returns
/// A vector of `RoamFile` representing the files referenced by the given file.
fn find_files_referenced_by(
    ref_stmt: &mut Statement,
    asset_stmt: &mut Statement,
    path: &RoamFile,
) -> anyhow::Result<ReferencedFiles> {
    debug!("Querying for files referenced by {}", path);
    let rows = ref_stmt.query_map([path], |row| {
        let file: RoamFile = row.get(0)?;
        debug!("Found referenced file: {}", file);
        Ok(file)
    })?;
    let notes: Vec<RoamFile> = rows.collect::<Result<_, _>>()?;

    let asset_rows = asset_stmt.query_map([path], |row| {
        let asset: RoamFile = row.get(0)?;
        debug!("Found referenced asset path: {}", asset);
        let asset = try_resolve_asset_path(path.as_ref(), asset.as_ref());
        if let Some(ref asset) = asset {
            debug!("Found referenced asset: {}", asset.display());
        }
        Ok(asset)
    })?;
    let assets = asset_rows
        .into_iter()
        .filter_map(|r| r.ok().flatten())
        .collect();

    Ok(ReferencedFiles { notes, assets })
}

/// A struct to hold the results of referenced files, separating notes and assets.
pub struct ReferencedFiles {
    /// The files associated with referenced node ids
    pub notes: Vec<RoamFile>,
    /// The files mentioned with `file:` links
    pub assets: Vec<PathBuf>,
}

/// Compute the transitive closure of files starting from a set of initial files,
/// following references in the org-roam database.
///
/// # Arguments
/// * `conn` - A connection to the org-roam SQLite database.
/// * `paths` - A slice of `RoamFile` representing the initial set of files to start from.
/// * `exclude` - A function that takes a `RoamFile` and returns `true` if the file should be excluded from the results.
pub fn find_file_references_recursive(
    conn: &Connection,
    paths: &[RoamFile],
    exclude: impl Fn(&RoamFile) -> bool,
) -> anyhow::Result<ReferencedFiles> {
    let mut ref_stmt = conn.prepare(r#"
        WITH source_file_node AS (SELECT nodes.id from nodes join files on nodes.file = files.file where files.file = ?1),
             referenced_nodes AS (SELECT dest from links, source_file_node where links.source = source_file_node.id and links.type = '"id"')
        SELECT nodes.file from nodes, referenced_nodes where nodes.id = referenced_nodes.dest;
    "#).unwrap();
    info!("Prepared statement for finding referenced files");

    let mut asset_stmt = conn.prepare(r#"
        WITH source_file_node AS (SELECT nodes.id, nodes.file from nodes join files on nodes.file = files.file where files.file = ?1)
        SELECT dest from links, source_file_node where links.source = source_file_node.id and links.type = '"file"'
    "#).unwrap();

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut all_assets = HashSet::new();
    let mut notes = Vec::new();

    for path in paths {
        if visited.insert(path.clone()) {
            info!("Starting from file: {}", path);
            queue.push_back(path.clone());
            notes.push(path.clone());
        }
    }

    while let Some(current) = queue.pop_front() {
        debug!("Processing file: {}", current);
        if exclude(&current) {
            debug!("File {} does not pass filter, skipping", current);
            continue;
        }
        let ReferencedFiles {
            notes: refs,
            assets,
        } = find_files_referenced_by(&mut ref_stmt, &mut asset_stmt, &current)?;
        for referenced in refs {
            debug!("Found referenced file: {}", referenced);
            if visited.insert(referenced.clone()) {
                queue.push_back(referenced.clone());
                notes.push(referenced);
            }
        }
        all_assets.extend(assets);
    }

    let assets = all_assets.into_iter().collect();
    Ok(ReferencedFiles { notes, assets })
}

fn try_resolve_asset_path(note_file: &Path, asset: &Path) -> Option<PathBuf> {
    debug!(
        "try_resolve_asset_path: note_file={}, asset={}",
        note_file.display(),
        asset.display()
    );
    if asset.is_absolute() {
        return Some(asset.to_path_buf());
    }
    let combined = note_file.parent()?.join(asset).canonicalize().ok()?;
    Some(combined)
}
