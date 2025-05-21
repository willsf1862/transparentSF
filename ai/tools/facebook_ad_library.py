import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_VERSION = "v19.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}/ads_archive"


def fetch_ads(search_terms, country="US", ad_type="POLITICAL_AND_ISSUE_ADS",
              limit=100, access_token=None, fields=None):
    """Fetch ads from the Facebook Ad Library API.

    Parameters
    ----------
    search_terms : str
        Text to search for in ad creative body.
    country : str, optional
        Country code for ads (default is "US").
    ad_type : str, optional
        Type of ads to return (default is "POLITICAL_AND_ISSUE_ADS").
    limit : int, optional
        Number of results per request (default is 100).
    access_token : str, optional
        Facebook API access token. If not provided, the function will attempt
        to read `FACEBOOK_ACCESS_TOKEN` from the environment.
    fields : list[str], optional
        Fields to include in the response.
    """
    access_token = access_token or os.getenv("FACEBOOK_ACCESS_TOKEN")
    if not access_token:
        raise ValueError("Facebook access token is required")

    params = {
        "access_token": access_token,
        "search_terms": search_terms,
        "ad_reached_countries": country,
        "ad_type": ad_type,
        "limit": limit,
    }
    if fields:
        params["fields"] = ",".join(fields)

    ads = []
    url = BASE_URL
    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        ads.extend(data.get("data", []))

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        # After the first request, parameters are embedded in the next URL
        url = next_url
        params = {}
    return ads


def save_ads_to_json(ads, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ads, f, ensure_ascii=False, indent=2)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Facebook Ad Library data")
    parser.add_argument("--search", required=True, help="Search terms")
    parser.add_argument("--country", default="US", help="Country code")
    parser.add_argument("--token", default=None, help="Access token")
    parser.add_argument("--limit", type=int, default=100,
                        help="Number of ads per request")
    parser.add_argument("--output", default="ai/data/facebook_ads/ads.json",
                        help="Output file for ads")
    parser.add_argument("--fields",
                        default=("page_id,page_name,ad_creative_body,"
                                 "ad_snapshot_url,impressions,spend,"
                                 "ad_delivery_start_time,ad_delivery_stop_time"),
                        help="Comma-separated list of fields to include")
    parser.add_argument("--ad-type", default="POLITICAL_AND_ISSUE_ADS",
                        help="Ad type filter")

    args = parser.parse_args()
    fields = [f.strip() for f in args.fields.split(",") if f.strip()]

    ads = fetch_ads(
        search_terms=args.search,
        country=args.country,
        ad_type=args.ad_type,
        limit=args.limit,
        access_token=args.token,
        fields=fields,
    )
    save_ads_to_json(ads, args.output)
    print(f"Saved {len(ads)} ads to {args.output}")


if __name__ == "__main__":
    main()

