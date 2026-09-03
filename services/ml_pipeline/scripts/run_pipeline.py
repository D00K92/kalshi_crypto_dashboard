"""Submit a compiled KFP pipeline to Vertex AI Pipelines."""
from __future__ import annotations

import argparse
from google.cloud import aiplatform


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default="volatility_training_pipeline.json")
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--pipeline-root", required=True)
    parser.add_argument("--service-account")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--model-version", default="v1")
    args = parser.parse_args()
    aiplatform.init(project=args.project, location=args.location)
    job = aiplatform.PipelineJob(
        display_name=f"crypto-volatility-training-{args.model_version}",
        template_path=args.template,
        pipeline_root=args.pipeline_root,
        parameter_values={"project": args.project, "location": args.location,
                          "start_date": args.start_date, "end_date": args.end_date,
                          "model_version": args.model_version},
        enable_caching=False,
    )
    job.run(service_account=args.service_account, sync=False)
    print(job.resource_name)


if __name__ == "__main__":
    main()
