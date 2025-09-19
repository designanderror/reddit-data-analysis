import os, json, traceback
from typing import Iterable
from fileStreams import getFileJsonStream
from utils import FileProgressLog

# ---------------- CONFIG ----------------
fileOrFolderPath = r"../../testdata"
recursive = True
target_subreddits = {"selfhosting", "selfhosted", "homelab", "homeserver", "homenetworking"}

output_dir = "../../filtered_output"
os.makedirs(output_dir, exist_ok=True)
submissions_prefix, comments_prefix = "filtered_submissions", "filtered_comments"
rows_per_file = 100_000
batch_size = 100
error_log_file = os.path.join(output_dir, "errors.log")
progress_file = os.path.join(output_dir, "progress.log")



class SplitWriter:
    """Writes JSONL to split files immediately without keeping a batch in memory."""
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.file_index = 0
        self.row_count = 0
        self.out = None
        self._open_new_file()

    def _open_new_file(self):
        if self.out:
            self.out.close()
        filename = os.path.join(output_dir, f"{self.prefix}_{self.file_index:04}.jsonl")
        self.out = open(filename, "a", encoding="utf-8")
        self.row_count = 0
        self.file_index += 1

    def write(self, row: dict):
        self.out.write(json.dumps(row) + "\n")
        self.row_count += 1
        if self.row_count >= rows_per_file:
            self._open_new_file()

    def close(self):
        if self.out:
            self.out.close()


def log_error(file: str, row_idx: int, exc: Exception):
    with open(error_log_file, "a", encoding="utf-8") as elog:
        elog.write(f"[{type(exc).__name__}] Error in {file} at row {row_idx}: {exc}\n")
        elog.write(traceback.format_exc() + "\n")


def save_progress(filename: str, row_idx: int):
    with open(progress_file, "w", encoding="utf-8") as plog:
        plog.write(f"{filename}\t{row_idx}\n")


def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as plog:
            line = plog.readline().strip()
            if line:
                file, row_idx = line.split("\t")
                return file, int(row_idx)
    return None, 0


def detect_file_type(path: str) -> str:
    base = os.path.basename(path).lower()
    if "rs_" in base:
        return "submission"
    if "rc_" in base:
        return "comment"
    return "unknown"


def processFile(path: str, start_row=0):
    ftype = detect_file_type(path)
    if ftype == "unknown":
        print(f"Skipping {path} (not RC_ or RS_)")
        return

    print(f"Processing {path} [{ftype}]")
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown format {path}")
            return

        progressLog = FileProgressLog(path, f)
        prefix_map = {"submission": submissions_prefix, "comment": comments_prefix}
        writer = SplitWriter(prefix_map[ftype])

        for i, row in enumerate(jsonStream):
            if i < start_row:
                continue
            try:
                progressLog.onRow()
                subreddit = row.get("subreddit", "").lower()
                if subreddit not in target_subreddits:
                    continue
                writer.write(row)

                if i % 1000 == 0:
                    save_progress(path, i)

            except Exception as e:
                log_error(path, i, e)

        writer.close()
        progressLog.logProgress("\n")


def processFolder(path: str, start_file=None, start_row=0):
    def iter_files(root_path: str) -> Iterable[str]:
        if recursive:
            for root, _, files in os.walk(root_path):
                for file in files:
                    yield os.path.join(root, file)
        else:
            for file in os.listdir(root_path):
                yield os.path.join(root_path, file)

    started = start_file is None
    for file in iter_files(path):
        if not started:
            if os.path.abspath(file) == os.path.abspath(start_file):
                started = True
                processFile(file, start_row)
        else:
            processFile(file)



def main():
    last_file, last_row = load_progress()
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath, last_file, last_row)
    else:
        processFile(fileOrFolderPath, last_row if last_file else 0)

    print("Done :>")


if __name__ == "__main__":
    main()
