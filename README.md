# gads-iac

Infrastructure-as-Code for Google Ads. Manage campaigns declaratively via YAML files and apply changes through a plan/apply workflow — similar to Terraform but for Google Ads.

## Commands

```
gads-iac import --account-id <ID>   Fetch live state and write local YAML files
gads-iac plan                        Show diff between local files and live state
gads-iac apply                       Apply local changes to Google Ads
gads-iac export-schema               Write gads-schema.json to the current directory
```

## Setup

The following environment variables are required for `import`, `plan`, and `apply`:

| Variable | Description |
|---|---|
| `GOOGLE_ADS_DEVELOPER_TOKEN` | Developer token from Google Ads API Center |
| `GOOGLE_PROJECT_ID` | GCP project ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a service account JSON key file |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | Manager account ID (defaults to `GOOGLE_PROJECT_ID`) |

`export-schema` requires no credentials.

### Getting a developer token

You need a **Google Ads Manager Account** (MCC) to get a developer token.

1. Create a Manager Account at [ads.google.com](https://ads.google.com) if you don't have one
2. In the Manager Account go to **Admin → API Center**
3. Apply for API access — basic access is approved immediately for test use; standard access (for production) requires a short application
4. Copy the developer token shown on that page

### Getting a service account key

1. Open [console.cloud.google.com](https://console.cloud.google.com) and select your GCP project
2. Go to **IAM & Admin → Service Accounts** and create a service account (or use an existing one)
3. Click the service account → **Keys** tab → **Add Key → Create new key → JSON**
4. Save the downloaded JSON file somewhere safe
5. Set the environment variable to its path:
   ```sh
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   ```
6. Grant the service account access to your Google Ads account:
   - In Google Ads go to **Admin → Access and security**
   - Invite the service account's email address (e.g. `my-sa@my-project.iam.gserviceaccount.com`) as a **Standard** user

## Workflow

```sh
# 1. Pull current state from Google Ads
gads-iac import --account-id 123-456-7890

# 2. Edit the generated YAML files
$EDITOR 123-456-7890_*_campaign.yaml

# 3. Preview what will change
gads-iac plan

# 4. Apply the changes
gads-iac apply
```

Import writes one file per campaign named `{account_id}_{campaign_id}_campaign.yaml`. All YAML files in the working directory are picked up by `plan` and `apply`.

---

## Campaign file format

Each file describes a single campaign. Only `name` and `status` are required; everything else is optional or defaults to empty.

```yaml
# yaml-language-server: $schema=gads-schema.json
name: My Campaign
status: ENABLED
daily_budget: 50.00
start_date: "2024-01-01"
end_date: "2024-12-31"

bidding_strategy:
  type: TargetCpa
  target_cpa: 10.00

locations:
  - geo_target_constant: geoTargetConstants/2840   # United States

callouts:
  - text: Free Shipping
  - text: 24/7 Support

sitelinks:
  - link_text: Shop Now
    final_urls:
      - https://example.com/shop
    line1: Browse our full range
    line2: New arrivals weekly

negative_keywords:
  - "[free]"
  - "\"cheap shoes\""

ad_groups:
  - name: Shoes - Exact
    status: ENABLED
    keywords:
      - "[buy running shoes]"
      - "[running shoes online]"
    negative_keywords:
      - free
    ads:
      - headlines:
          - Running Shoes
          - Shop Running Shoes
          - Free Shipping Over $50
        descriptions:
          - Top brands, fast delivery. Shop now.
          - Find the perfect pair today.
        final_urls:
          - https://example.com/running-shoes
```

### Keyword match types

Keywords are encoded as plain strings using bracket notation:

| Notation | Match type | Example |
|---|---|---|
| `[keyword]` | Exact | `[buy running shoes]` |
| `"keyword"` | Phrase | `"running shoes"` |
| `keyword` | Broad | `running shoes` |

### Bidding strategies

The `type` field selects the strategy. Available options:

```yaml
# Target CPA
bidding_strategy:
  type: TargetCpa
  target_cpa: 15.00        # in account currency

# Target ROAS
bidding_strategy:
  type: TargetRoas
  target_roas: 4.0         # ratio — 4.0 = 400%

# Maximize Conversions (optional CPA target)
bidding_strategy:
  type: MaximizeConversions
  target_cpa: 10.00        # optional

# Maximize Conversion Value (optional ROAS target)
bidding_strategy:
  type: MaximizeConversionValue
  target_roas: 3.5         # optional

# Manual CPC
bidding_strategy:
  type: ManualCpc
  enhanced_cpc_enabled: true
```

### IDs

Fields named `id` (`campaign.id`, `ad_group.id`, `ad.id`) are set by `import` and used internally for mutations. You can omit them when writing campaigns from scratch — they will be assigned by Google Ads on `apply`.

---

## Schema

Run `gads-iac export-schema` to generate `gads-schema.json` in the current directory. Adding a modeline comment to your YAML files enables validation and autocomplete in editors with the yaml-language-server (VS Code, IntelliJ, etc.):

```yaml
# yaml-language-server: $schema=gads-schema.json
name: My Campaign
status: ENABLED
```

To regenerate the schema after upgrading:

```sh
gads-iac export-schema
```

The schema is derived directly from the same types used for serialization, so it is always in sync with what `plan` and `apply` accept.
