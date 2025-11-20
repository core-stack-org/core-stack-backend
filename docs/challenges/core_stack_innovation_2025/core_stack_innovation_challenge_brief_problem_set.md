# CoRE Stack Innovation Challenge: Nov 2025

Inviting ecologists, water-security researchers, geospatial programmers, and community practitioners to build open-source solutions, analyses, and tools using the CoRE Stack datasets and APIs to improve socio-ecological resilience and support community-led landscape action.

---

## 1. Overview
The [CoRE Stack](https://core-stack.org/) (Commoning for Resilience and Equality) is a community-based digital public infrastructure of pre-computed spatial datasets, analytic pipelines, and user-facing tools that help rural communities, researchers, and practitioners understand and act on socio-ecological challenges. This challenge asks teams to use the CoRE Stack APIs and datasets to: 1) answer scientific or operational questions about landscapes; 2) propose meaningful additions or integrations for the stack; and 3) deliver open-source code, reproducible notebooks, and demonstrations that can be adopted by practitioners or integrated into the CoRE stack.

Participants are encouraged to form interdisciplinary teams that combine domain expertise (ecology, hydrology, social science) with geospatial programming and UX/dev skills.

---

## 2. Who should participate
- Ecologists and hydrologists
- Researchers in water security and land use
- Geospatial engineers, data scientists, software developers
- NGOs, community technologists, and practitioners working with rural landscapes
- Students and open-source contributors interested in socio-ecological data

---

## 3. Deliverables (examples)
A valid submission should include:
1. A public Git repository (GitHub/GitLab) with source code, scripts, notebooks, and documentation.
2. At least one reproducible notebook or script that demonstrates fetching CoRE Stack data via the CoRE APIs and produces the core analysis or visualization.
3. A short demo video (3–6 minutes) showing the tool/analysis and the main findings. Or, a short PDF report with sample outputs and conclusions from the analysis. 
4. README with clear setup and run instructions, license (prefer permissive open-source license), and a short project summary.

Each submission should also include a `metadata.json` (one-page) with: team name, members, affiliations, contact email, short abstract (max 250 words), problem chosen, and link to the repo and demo video.

---

## 4. Challenge types
Teams can submit entries for multiple types of challenges but clearly separate out the different pieces:

### A. Data Exploration & Insights
Use CoRE Stack layers to answer concrete landscape questions (comparative analyses, trend detection, counterfactuals). Deliver reproducible notebooks, maps, and concise interpretative notes for practitioners. Several such problems are outlined in the next section. 

### B. Tooling, APIs & Developer Tools
Build developer-facing tools or libraries that make the CoRE Stack easier to use (client libraries, wrappers, STAC tools, boundary-clip and vectorization utilities, etc.).

### C. Data stories & UX Integrations
Create user-facing dashboards, chatbots, WhatsApp-shareable slide generators, or lightweight tools that practitioners and village communities can easily use to tell data-based stories. Focus on clarity, localization, and actionable outputs.

---

## 5. Example problem statements for Challenge Type A - Data Exploration and Insights. Pick / adapt any
> _Note: Participants should carefully first examine the data and build an intuition for the CoRE stack approach, and then begin solving._

### Boundary flexibility problems
1. CoRE stack provides analytics at the micro-watershed level by default but users often want the same stats at the village level computed from the underlying rasters. For a given village polygon, using the underlying LULC rasters over the years, compute and visualize how cropping intensity (single/double/triple) has changed over the years. Provide stacked-area or stacked-bar charts and a downloadable CSV with annual areas.
2. For the same village, compute seasonal surface-water availability (perennial, winter, monsoon) across years and show trends in area (hectares) per season.
3. Identify areas within a village that lost tree cover over a chosen period and estimate hectares of degraded land.

### Problems needing composition of multiple layers
4. Rank micro-watersheds in a tehsil by cropping sensitivity to drought (compare cropping intensity during identified drought years vs non-drought years to find the micro-watersheds most sensitive to drought).
5. Find the top 5 micro-watersheds most similar to a reference micro-watershed based on terrain, drought frequency, and land-use (use a distance function or propensity score matching methods to identify counterfactual micro-watersheds). Compare their water availability and cropping intensity and surface practice indicators. Differences could highlight positive deviant micro-watersheds worth investigating further on what practices could the local communities be using to be in a better social-ecological position. 
6. For each micro-watershed in a tehsil, build a four-quadrant classification using mean cropping intensity (high/low) and mean runoff (high/low) to identify priority zones for water-harvesting or crop intensification. Areas with low cropping intensity but significant surplus runoff could benefit from rainwater harvesting structures to conserve water during the monsoons and utilize it later for a second crop. 
7. In a tehsil, compare % SC/ST population by village against the number/volume of NREGA works and visualize potential gaps or correlations that could help spot potentially marginalized villages where NREGA is not very actively used.

### Problems requiring integration with external data
8. Using an external dataset of market locations (e.g., APMC mandi points), analyze whether proximity to markets correlates with cropping intensity and produce visualizations and statistical tests.

---

## 6. Resources & starting points
- API key generation: https://core-stack.org/use-apis/
- API documentation: https://api-doc.core-stack.org/
- Example notebooks (API + plotting examples):
  - Rainfall/ET/runoff water-balance notebook (Colab)
  - Cropping intensity by micro-watershed notebook (Colab)
- STAC endpoints and layer catalog (collections by state/district/tehsil): https://stac.core-stack.org/
- CoRE Stack technical manual (datasets & methods): https://core-stack.org/core-stack-technical-manual-v2/

Teams are expected to register for an API key. The very first API you should try to use is XXX to get the list of locations for which CoRE stack data is populated. 

---

## 7. Evaluation criteria
Submissions will be scored on:
1. **Relevance & impact** — Does the work address a clear landscape problem and provide actionable insights for practitioners? (25%)
2. **Technical quality** — Correctness, reproducibility, code quality, and validation of methods. (20%)
3. **Innovation** — Novel use of datasets, creative modeling, or tooling that adds value. (20%)
4. **Usability & presentation** — Clarity of visualizations, interpretability for non-technical stakeholders, and quality of demo. (15%)
5. **Openness & reproducibility** — Public code, clear instructions, permissive license, and data provenance. (10%)
6. **Community value** — Potential to be adopted, extensible, and contributed back to the CoRE open-source ecosystem. (10%)

---

## 8. Submission guidelines
- Submit via the provided form (XXX) with repo and demo links. Attach `metadata.json` and point to the main notebook(s).
- All code should be open-source; include a LICENSE and clear run instructions. If you cannot publish certain datasets for legal/privacy reasons, include a script that downloads data via the CoRE APIs and a small synthetic dataset to let reviewers run the analysis.
- Provide at least one reproducible entrypoint (notebook or script) that downloads and processes CoRE Stack data automatically.

---

## 9. Prizes & support
- Monetary prizes for top 3 projects per track.
- Mentorship sessions with domain experts and CoRE Stack developers for top finalists.
- Opportunities to integrate winning work into the CoRE Stack ecosystem and technical support for adoption.

---

## 10. Timeline & milestones (template)
- Launch & registration opens: `T0`
- Midway developer community calls / mentorship: `T0 + 3 weeks`
- Submission deadline: `T0 + 6 weeks`
- Shortlist & demos: `T0 + 8 weeks`
- Winners announced & prize distribution: `T0 + 10 weeks`

---

## 11. Judging process & reproducibility checks
Judges will review code and run at least one primary notebook per submission to verify reproducibility. Submissions that require significant manual steps without automation will be penalized. Judges may invite finalists to provide a short live demo.

---

## 12. Contact & support
For discussions and help, join the XXX or Slack/Discord for questions, plus scheduled developer community calls for API onboarding and data orientation.

---

## 13. Appendix: Starter ideas for quick wins
- A small CLI tool that clips any CoRE raster by an uploaded village polygon and returns annual area statistics (cropping intensity, tree cover loss, water bodies area).
- A notebook that identifies drought years using rainfall anomaly thresholds and ranks micro-watersheds by cropping drop during those years.
- A mapping widget (static or lightweight web UI) that allows practitioners to toggle layers and export a WhatsApp-friendly slide summarizing the last 5 years of trends for a village.


---
