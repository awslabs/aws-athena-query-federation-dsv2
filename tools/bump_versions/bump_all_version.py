# Adapted from: https://github.com/awslabs/aws-athena-query-federation/blob/master/tools/bump_versions/bump_connectors_version.py
import os
import glob

from pathlib import Path
from bs4 import BeautifulSoup

import common

# Dependencies:
# pip3 install lxml beautifulsoup4
# sudo apt-get install libxml2-utils

EXCLUDE_DIRS={
}


if __name__ == "__main__":
    # Make sure we are in the root directory
    root_dir = Path(__file__).parent.parent.parent
    os.chdir(root_dir)

    new_version = common.get_new_version()

    module_dirs = [ "." ] + list(filter(lambda x: x not in EXCLUDE_DIRS, glob.glob("athena*")))

    module_artifact_ids = common.get_projects_artifact_ids_map(module_dirs)

    # Bump the versions across all of the modules including dependencies on other modules
    for module in module_dirs:
        with open(f"{module}/pom.xml") as f:
            soup = BeautifulSoup(f, 'xml')

            # First update the project's version
            common.update_project_version(soup, new_version)

            # Then update any dependencies on other modules to the new version as well
            for module_artifact_id in module_artifact_ids.values():
                # This is ourselves. We can't depend on ourselves so skip this.
                if module_artifact_id == module_artifact_ids[module]:
                    continue
                common.update_dependency_version(soup, module_artifact_id, new_version)

        # Output the xml
        common.output_xml(soup, f"{module}/pom.xml")

        # Bump the versions in the yaml files
        yaml_files = glob.glob(f"{module}/*.yaml") + glob.glob(f"{module}/*.yml")
        common.update_yaml(yaml_files, new_version)
