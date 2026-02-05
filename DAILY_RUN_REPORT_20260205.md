# Daily Run Analysis Report
**Date:** February 5, 2026  
**Run:** daily_run.py  
**Status:** Completed with errors

---

## Executive Summary

✅ **RSS Ingest:** SUCCESS  
✅ **Scoring Pipeline:** PARTIAL SUCCESS  
⚠️ **Overall Status:** COMPLETED_WITH_ERRORS

---

## Stage 1: RSS Ingest

**Status:** ✅ SUCCESS

| Metric | Count |
|--------|-------|
| Total items fetched | 25 |
| New records created | 23 |
| Duplicates skipped | 2 |
| First created ID | recsABWZgPtEwF2yg |

**Analysis:**
- RSS feed fetched successfully from configured source
- 92% of items were new (23/25)
- No ingest errors
- All records created with Title, JobURL, JobDescriptionRaw, Source, DateFound fields

---

## Stage 2: Score New Jobs

**Status:** ⚠️ PARTIAL SUCCESS

| Metric | Count | Percentage |
|--------|-------|------------|
| Jobs processed | 20 | 100% |
| Successfully scored | 17 | 85% |
| Failed scoring | 3 | 15% |

### Success Distribution by FitScore

| FitScore | Action | Count |
|----------|--------|-------|
| 1 (Skip) | Skip | 5 |
| 2 (Maybe) | Network First | 6 |
| 3 (Good) | Network First | 6 |

**Total actionable jobs (FitScore 2-3):** 12 out of 17 scored (71%)

### Detailed Failures

#### Failed Records (3 total)

1. **recPbjEJOnvgB82ek**
   - URL: https://www.linkedin.com/jobs/view/ai-ml-engineer-...
   - JD Length: 2,123 chars
   - JobJSON Size: 1,785 bytes
   - Fields Parsed: JobTitle, NeedsHumanInput, RemoteStatus, Requirements, Responsibilities, TechStack
   - Error: `FAIL_BAD_MODEL_OUTPUT` → `invalid_model_output`
   - Note: Company field missing from parse

2. **recbXiSOXuOFAo41C**
   - URL: https://www.linkedin.com/jobs/view/machine-learnin-...
   - JD Length: 4,136 chars
   - JobJSON Size: 1,377 bytes
   - Fields Parsed: Company, JobTitle, Keywords, NeedsHumanInput, RemoteStatus, Requirements, Responsibilities, TechStack
   - Flags: `gov_or_defense: true`
   - Error: `FAIL_BAD_MODEL_OUTPUT` → `invalid_model_output`

3. **recfWp7P6QCNl4VJH**
   - URL: https://www.linkedin.com/jobs/view/machine-learnin-...
   - JD Length: 6,247 chars (largest in batch)
   - JobJSON Size: 2,004 bytes
   - Fields Parsed: Company, JobTitle, Keywords, NeedsHumanInput, RemoteStatus, Requirements, Responsibilities, TechStack
   - Flags: `gov_or_defense: true`
   - Error: `FAIL_BAD_MODEL_OUTPUT` → `invalid_model_output`

---

## Failure Analysis

### Root Cause: Invalid Model Output

**Error Location:** [score_new_jobs.py](score_new_jobs.py#L251-L252)

The scoring engine (`v1`) is failing to produce valid JSON with required keys:
```python
required_keys = {
    "fit_score", "next_action", "fit_reasons", "gaps_risks",
    "non_obvious_matches", "keywords_to_tailor_resume",
    "questions_to_verify", "confidence", "needs_human_input", "debug"
}
```

**Failure Pattern:**
1. Initial LLM request returns invalid/incomplete JSON
2. Retry with strict instruction also fails
3. System marks as `invalid_model_output` error

### Potential Causes

1. **Prompt Length/Complexity:**
   - 2 of 3 failures have `gov_or_defense: true` flag
   - Largest job description (6,247 chars) failed
   - May be hitting context window or complexity limits

2. **Model Instability:**
   - 15% failure rate suggests intermittent LLM response issues
   - v1 prompt may not be robust enough for edge cases

3. **Company Field Correlation:**
   - recPbjEJOnvgB82ek missing Company field in parse
   - May indicate upstream parsing issues affecting scoring

### Warning Patterns

**Short Job Descriptions:** 5 jobs flagged with `WARN_SHORT_JD` (< 200 chars)
- All auto-scored as FitScore=1 (Skip)
- All marked with `insufficient_description` in NeedsHumanInput
- This is expected behavior for LinkedIn snippet-only postings

**JobJSON Parse Failures:** 2 jobs
- recKFisdyPVDrGM11: `JOBJSON_PARSE_FAILED` but recovered with fallback
- recgZGL1Ii74dhIxM: `JOBJSON_PARSE_FAILED` but recovered with fallback
- Both successfully scored after fallback

---

## Company/Location Population Status

### From Scored Jobs (17 successful)

**Company Field:**
- Populated via JobJSON parsing: 13/17 (76%)
- Missing: 4/17 (24%)

**Location Field:**
- Populated via JobJSON parsing: 5/17 (29%)
- Missing: 12/17 (71%)

**Analysis:**
- Company extraction is working reasonably well (76% populated)
- Location extraction needs improvement (only 29% populated)
- Both rely on LLM-based JobJSON parsing from job descriptions
- RSS metadata extraction rollback confirmed: RSS feeds only provide Title/URL/Description

---

## Recommendations

### Immediate Actions

1. **Re-run Failed Jobs:**
   ```bash
   python src/score_new_jobs.py --filter-step 2 --max 3
   ```
   This will target records with JobJSON but no FitScore

2. **Investigate v1 Prompt:**
   - Review [prompts/job_scorer_v1.txt](prompts/job_scorer_v1.txt)
   - Test prompt with failed job records manually
   - Consider adding more explicit JSON structure examples

### Short-term Improvements

1. **Enhanced Error Recovery:**
   - Add third retry attempt with minimal prompt
   - Implement graceful degradation (score as 2/Maybe if invalid output)
   - Log full LLM response for debugging

2. **Scoring Robustness:**
   - Test `perfecter_v1` prompt on failed records
   - Consider A/B testing on all jobs to compare failure rates
   - Add timeout protection for very long descriptions

3. **Location Population:**
   - Location is 71% missing from LLM-parsed JobJSON
   - Consider post-processing enhancement:
     - Add fallback extraction from job titles (e.g., "Engineer - San Francisco")
     - Parse location from remaining JobDescriptionRaw text
     - Use company headquarters data when Remote is not set

### Long-term Enhancements

1. **Monitoring Dashboard:**
   - Track scoring failure rate over time
   - Alert if >20% failure rate
   - Track Company/Location population percentages

2. **Prompt Engineering:**
   - Iterate on v1 prompt based on failure examples
   - Add schema validation examples to prompt
   - Test newer models for JSON reliability

3. **Alternative Extraction:**
   - For location: LinkedIn API or web scraping as fallback
   - For company: Use domain-to-company mapping service

---

## Pipeline Health Metrics

| Metric | Value | Status |
|--------|-------|--------|
| RSS Ingest Success Rate | 100% | ✅ Excellent |
| Scoring Success Rate | 85% | ⚠️ Needs attention |
| Company Population | 76% | ✅ Good |
| Location Population | 29% | ⚠️ Needs improvement |
| Actionable Jobs Rate | 71% | ✅ Good |

---

## Next Steps

1. ✅ RSS ingest is stable - no changes needed
2. ⚠️ Address 15% scoring failure rate
3. ⚠️ Improve location extraction to >70%
4. 📊 Run daily monitoring for 7 days to establish baseline

**Priority:** Medium - System is functional but could be more robust
