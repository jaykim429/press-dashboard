# Insurance Impact Report (Example, LLM-free)

- generation_mode: rule_based_template
- analysis_period: 2026-03-05 ~ 2026-03-06
- source_count: 7

## Step 1. Document Type + Summary

| type | count |
|---|---:|
| recent_rule_change_info | 5 |
| admin_guidance_notice | 1 |
| law_interpretation | 1 |

Summary:
- The recent batch includes regulation notice, administrative guidance, and legal interpretation sources.
- This mix implies potential impact on compliance process and policy interpretation.
- Evidence: [source_index:1], [source_index:3], [source_index:5]

## Step 2. Relevance Score (Life Insurance)

- relevance_score: **8/10**
- rationale:
  - regulation/guidance/legal interpretation combination can affect product disclosure, sales governance, and compliance controls.
  - evidence: [source_index:1], [source_index:3], [source_index:5]

## Step 3. Impact Analysis

| area | impact | assessment |
|---|---|---|
| finance_sales | medium | possible updates in operating standards may affect sales process and cost of compliance |
| system_it | medium | potential need to update reporting/control fields and audit logs |
| org_process | high | internal policy/checklist update likely required across compliance + business teams |

## Step 4. Priority + Action Plan

- priority: **high**

| action | owner | target | evidence |
|---|---|---|---|
| build regulation delta sheet (before/after) | compliance | D+2 | [source_index:1], [source_index:3] |
| check product-disclosure and sales-material impact | product + sales support | D+5 | [source_index:1], [source_index:6] |
| review internal control and complaint process changes | compliance + consumer protection | D+7 | [source_index:3], [source_index:5] |
| identify IT/data-control change points | IT + data governance | D+7 | [source_index:1], [source_index:3] |

## Source List

- [source_index:1] article_id=15604, source_channel=fsc_regulation_notice
- [source_index:2] article_id=15604, source_channel=fsc_regulation_notice
- [source_index:3] article_id=15494, source_channel=fss_admin_guidance_notice
- [source_index:4] article_id=15193, source_channel=kofia_recent_rule_change
- [source_index:5] article_id=14411, source_channel=fsc_law_interpretation
- [source_index:6] article_id=14208, source_channel=fsc_regulation_notice
- [source_index:7] article_id=8443, source_channel=fsc_regulation_notice
