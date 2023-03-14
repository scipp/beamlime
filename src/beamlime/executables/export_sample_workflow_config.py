from ..resources.exporters import export_sample_workflow_yaml

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="./")
    parser.add_argument("--name", default="sample-workflow.yaml")
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    export_sample_workflow_yaml(
        directory=args.directory, filename=args.name, overwrite=args.overwrite
    )
