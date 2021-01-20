from datetime import date
from pathlib import Path
from typing import Optional
import io
import re
import shutil

from .cmd import collect_output, stream_output
from .common import ReleaseFile


class Repository:
    def __init__(self, path: Path, repository_url: str):
        self.path = path
        self.repository_url = repository_url

    def clone(self, branch: Optional[str] = None):
        """Clone the given branch into path. Initialize the dbt user so commits
        work.
        """
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        cmd = ["git", "clone"]
        if branch is not None:
            cmd.extend(["--branch", branch])
        cmd.extend([self.repository_url, str(self.path)])
        stream_output(cmd)

    def checkout_branch(self, branch: str, *, new: bool = False):
        cmd = ["git", "checkout"]
        if new:
            cmd.append("-b")
        cmd.append(branch)
        stream_output(cmd, cwd=self.path)

    def _rev_parse(self, commitish: str) -> str:
        cmd = ["git", "rev-parse", commitish]
        return collect_output(cmd, cwd=self.path).strip()

    def push_updates(self, *, origin_name: Optional[str] = None):
        cmd = ["git", "push"]
        if origin_name:
            cmd.extend(["origin", origin_name])
        stream_output(cmd, cwd=self.path)

    def merge(self, merge_from: str):
        # if something has merged during our build, we should fail.
        cmd = ["git", "merge", "--no-ff", "origin/" + merge_from]
        stream_output(cmd, cwd=self.path)


class DbtRepository(Repository):
    def __init__(self, path: Path):
        url = "git@github.com:fishtown-analytics/dbt.git"
        super().__init__(path=path, repository_url=url)

    def ensure_matching_commits(self, release: ReleaseFile):
        commit_result = self._rev_parse(release.commit)
        branch_result = self._rev_parse(release.branch)
        if commit_result != branch_result:
            raise ValueError(
                f"Commit {release.commit} points to sha {commit_result}, "
                f"but branch {release.branch} points to sha {branch_result}"
            )
        if not commit_result.startswith(release.commit):
            raise ValueError(
                f"Sha {commit_result} does not start with {release.commit}"
            )

    def set_version(self, release: ReleaseFile, env_path: Path) -> None:
        bin_path = env_path / "bin/bumpversion"
        # if we always set the version to major, this appears to always work...
        cmd = [
            bin_path.absolute(),
            "--commit",
            "--no-tag",
            "--new-version",
            str(release.version),
            "major",
        ]
        print(f"bumping version to {release.version}")
        print(f"running cmd: {cmd}")
        stream_output(cmd, cwd=self.path)
        print(f"bumped version to {release.version}")

    def add_requirements_txt(self, requirements_path: Path) -> None:
        print(f"Adding requirements file at {requirements_path}")
        cmd = ["git", "add", str(requirements_path)]
        stream_output(cmd, cwd=self.path)
        cmd = ["git", "commit", "-m", "Add requirements file"]
        stream_output(cmd, cwd=self.path)
        print("Added requirements file")

    def get_commit_hash(self) -> str:
        # get the commit hash out
        cmd = ["git", "rev-parse", "--short", "HEAD"]
        new_commit_hash = collect_output(cmd, cwd=self.path).strip()
        return new_commit_hash

    def update_changelog(self, release: ReleaseFile):
        path = self.path / "CHANGELOG.md"

        nextver = re.compile(r"^## dbt [0-9]+\.[0-9]+\.[0-9]+ \((Release TBD)\)")

        newdata = io.StringIO()

        release_date = date.today().strftime("%B %d, %Y")

        print("Updating the changelog")

        with open(path, "r") as fp:
            firstline = fp.readline()
            match = nextver.match(firstline)
            if match is None:
                raise ValueError(f"Unexpected first line of CHANGELOG.md: {firstline}")
            if release.is_prerelease:
                # if our release is a prerelease, keep the existing line
                newdata.write(firstline)
                newdata.write("\n")
                newdata.write(f"## dbt {release.version} ({release_date})\n")
                newdata.write("\n")
            else:
                newdata.write(firstline.replace("Release TBD", release_date))
            newdata.write(fp.read())

        with open(path, "w") as fp:
            fp.write(newdata.getvalue())

        print("Adding changelog update")
        cmd = ["git", "add", "CHANGELOG.md"]
        stream_output(cmd, cwd=self.path)

        print("Committing changelog update")
        cmd = ["git", "commit", "-m", "update CHANGELOG.md"]
        stream_output(cmd, cwd=self.path)
        print("Committed changelog update")

    def merge_commits_with_version(self, release: ReleaseFile):
        cmd = ["git", "reset", "--soft", "HEAD~3"]
        stream_output(cmd, cwd=self.path)
        cmd = ["git", "commit", "-m", f"Release dbt v{release.version}"]
        stream_output(cmd, cwd=self.path)

    def perform_version_update(
        self, release: ReleaseFile, requirements_path: Path, packaging_venv: Path
    ) -> str:
        """Perform the git-tracked tasks involved in a version update and
        return the new commit hash.
        """
        # make sure things are reasonable
        self.ensure_matching_commits(release)
        self.set_version(release, packaging_venv)
        self.update_changelog(release)
        self.add_requirements_txt(requirements_path)
        self.merge_commits_with_version(release)
        return self.get_commit_hash()


class HomebrewRepository(Repository):
    def __init__(self, path: Path):
        url = "git@github.com:fishtown-analytics/homebrew-dbt.git"
        super().__init__(path=path, repository_url=url)
