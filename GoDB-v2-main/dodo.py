import os
from glob import glob

LABS = 5


def read_manifest(manifest):
    with open(manifest) as f:
        return [line.strip() for line in f]


def write_manifest(manifest, files):
    with open(manifest, "w") as f:
        f.write("\n".join(files))


def contents(path):
    return [f for f in glob(path + "/**/*", recursive=True) if os.path.isfile(f)]


def task_labs():
    for lab in range(1, LABS + 1):
        lab_name = f"lab{lab}"

        yield {
            "name": lab_name,
            "file_dep": [f"{lab_name}-ro.manifest", f"{lab_name}-rw.manifest"]
            + read_manifest(f"{lab_name}-ro.manifest")
            + read_manifest(f"{lab_name}-rw.manifest"),
            "actions": [
                f"cat {lab_name}-rw.manifest {lab_name}-ro.manifest > {lab_name}.manifest",
                f"perl strip.pl {lab_name} {lab_name}.manifest dist/",
                f"cd dist/{lab_name} && zip -r ../{lab_name}.zip *",
            ],
            "targets": [
                f"{lab_name}.manifest",
                f"dist/{lab_name}",
                f"dist/{lab_name}.zip",
            ],
        }

        if lab > 1:
            manifests = []
            for prev_lab in range(1, lab + 1):
                manifests.append(f"lab{prev_lab}-ro.manifest")
                manifests.append(f"lab{prev_lab}-rw.manifest")
            files = []
            for manifest in manifests:
                files += read_manifest(manifest)
            files = list(set(files))

            yield {
                "name": lab_name + "-full-starter",
                "file_dep": manifests + files,
                "actions": [
                    f"rm -rf dist/lab{lab}-full-starter dist/{lab_name}",
                    (write_manifest, [f"{lab_name}-full-starter.manifest", files]),
                    f"perl strip.pl {lab_name} {lab_name}-full-starter.manifest dist/",
                    f"mv dist/{lab_name} dist/lab{lab}-full-starter",
                ],
                "targets": [f"dist/lab{lab}-full-starter"],
            }


def task_autograders():
    for lab_num in range(1, LABS + 1):
        lab = f"lab{lab_num}"
        yield {
            "name": lab,
            "actions": [
                f"rm -rf dist/auto-{lab}",
                f"mkdir -p dist/auto-{lab}",
                f"perl strip.pl {lab} {lab}-ro.manifest dist/auto-{lab}/",
                f"mv dist/auto-{lab}/{lab}/* dist/auto-{lab} && rmdir dist/auto-{lab}/{lab}",
                f"rsync --files-from={lab}-hi.manifest -r . dist/auto-{lab}/",
                f"cp autograder/* dist/auto-{lab}/",
                f"cp {lab}-score.json dist/auto-{lab}/score.json",
                f"cd dist/auto-{lab} && zip -r ../auto-{lab}.zip .",
            ],
            "file_dep": [f"{lab}-score.json"]
            + read_manifest(f"{lab}-ro.manifest")
            + read_manifest(f"{lab}-hi.manifest")
            + contents("autograder"),
            "targets": [f"dist/auto-{lab}.zip"],
        }


def task_autograders_docker():
    for lab_num in range(1, LABS + 1):
        lab = f"lab{lab_num}"
        yield {
            "name": lab,
            "actions": [
                f"rm -rf dist/auto-{lab}",
                f"mkdir -p dist/auto-{lab}",
                f"rsync --files-from={lab}-ro.manifest -r . dist/auto-{lab}/",
                f"rsync --files-from={lab}-hi.manifest -r . dist/auto-{lab}/",
                f"cp autograder/* dist/auto-{lab}/",
                f"cp {lab}-score.json dist/auto-{lab}/score.json",
                f'docker build -t jfeser/hamilton-cpsci-350:{lab} --build-arg="SOURCE_DIR=dist/auto-{lab}" .',
            ],
            "file_dep": [f"{lab}-score.json", "Dockerfile"]
            + read_manifest(f"{lab}-ro.manifest")
            + read_manifest(f"{lab}-hi.manifest")
            + contents("autograder"),
        }
