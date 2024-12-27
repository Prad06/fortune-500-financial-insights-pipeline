import argparse
import json
import logging

from google.cloud import storage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_from_gcs(bucket_name, file_name):
    """Read a JSON file from GCS."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_text()
        logger.info(f"Successfully read {file_name} from GCS bucket {bucket_name}")
        return json.loads(data)
    except Exception as e:
        logger.error(f"Error reading from GCS: {str(e)}")
        raise


def write_to_gcs(bucket_name, file_name, data):
    """Write JSON data to a file in GCS."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Convert data to JSONL format
        json_str = json.dumps(data)
        blob.upload_from_string(json_str)

        logger.info(f"Successfully wrote file to gs://{bucket_name}/{file_name}")
    except Exception as e:
        logger.error(f"Error writing to GCS: {str(e)}")
        raise


def standardize_schema(data):
    """Standardize the schema of the JSON data."""
    try:
        standardized_data = {
            "adult": data.get("esgScores", {}).get("adult", ""),
            "alcoholic": data.get("esgScores", {}).get("alcoholic", ""),
            "animalTesting": data.get("esgScores", {}).get("animalTesting", ""),
            "catholic": data.get("esgScores", {}).get("catholic", ""),
            "coal": data.get("esgScores", {}).get("coal", ""),
            "controversialWeapons": data.get("esgScores", {}).get(
                "controversialWeapons", ""
            ),
            "environmentPercentile": data.get("esgScores", {}).get(
                "environmentPercentile", ""
            ),
            "environmentScore": data.get("esgScores", {}).get("environmentScore", ""),
            "esgPerformance": data.get("esgScores", {}).get("esgPerformance", ""),
            "furLeather": data.get("esgScores", {}).get("furLeather", ""),
            "gambling": data.get("esgScores", {}).get("gambling", ""),
            "governanceScore": data.get("esgScores", {}).get("governanceScore", ""),
            "maxAge": data.get("esgScores", {}).get("maxAge", ""),
            "militaryContract": data.get("esgScores", {}).get("militaryContract", ""),
            "nuclear": data.get("esgScores", {}).get("nuclear", ""),
            "palmOil": data.get("esgScores", {}).get("palmOil", ""),
            "peerCount": data.get("esgScores", {}).get("peerCount", ""),
            "peerEnvironmentPerformance": data.get("esgScores", {})
            .get("peerEnvironmentPerformance", {})
            .get("avg", ""),
            "peerEsgScorePerformance": data.get("esgScores", {})
            .get("peerEsgScorePerformance", {})
            .get("avg", ""),
            "peerGovernancePerformance": data.get("esgScores", {})
            .get("peerGovernancePerformance", {})
            .get("avg", ""),
            "peerGroup": data.get("esgScores", {}).get("peerGroup", ""),
            "peerHighestControversyPerformance": data.get("esgScores", {})
            .get("peerHighestControversyPerformance", {})
            .get("avg", ""),
            "peerSocialPerformance": data.get("esgScores", {})
            .get("peerSocialPerformance", {})
            .get("avg", ""),
            "percentile": data.get("esgScores", {}).get("percentile", ""),
            "pesticides": data.get("esgScores", {}).get("pesticides", ""),
            "ratingMonth": data.get("esgScores", {}).get("ratingMonth", ""),
            "ratingYear": data.get("esgScores", {}).get("ratingYear", ""),
            "smallArms": data.get("esgScores", {}).get("smallArms", ""),
            "socialScore": data.get("esgScores", {}).get("socialScore", ""),
            "tobacco": data.get("esgScores", {}).get("tobacco", ""),
            "totalEsg": data.get("esgScores", {}).get("totalEsg", ""),
        }

        logger.info("Successfully standardized data schema")
        return standardized_data
    except Exception as e:
        logger.error(f"Error standardizing schema: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Transform JSON Data")
    parser.add_argument(
        "--input-bucket", required=True, help="GCS bucket containing input file"
    )
    parser.add_argument("--input-file", required=True, help="Input JSON file in GCS")
    parser.add_argument(
        "--output-bucket", required=True, help="GCS bucket for output file"
    )
    parser.add_argument("--output-file", required=True, help="Output JSON file in GCS")

    args = parser.parse_args()
    logger.info(f"Starting transformation with arguments: {args}")

    try:
        # Read data from GCS
        logger.info(f"Reading data from gs://{args.input_bucket}/{args.input_file}")
        data = read_from_gcs(args.input_bucket, args.input_file)

        # Transform the data
        logger.info("Transforming data...")
        standardized_data = standardize_schema(data)

        # Write transformed data to GCS
        logger.info(
            f"Writing transformed data to gs://{args.output_bucket}/{args.output_file}"
        )
        write_to_gcs(args.output_bucket, args.output_file, standardized_data)

        logger.info("Transformation completed successfully")

    except Exception as e:
        logger.error(f"Failed to process file: {str(e)}")
        raise


if __name__ == "__main__":
    main()
