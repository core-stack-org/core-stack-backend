
# CoRE Stack Innovation Challenge: Nov 2025

Inviting ecologists, water-security researchers, geospatial programmers, and community practitioners to build open-source solutions, analyses, and tools using the CoRE Stack datasets and APIs to improve socio-ecological resilience and support community-led landscape action.

---

## 1. Overview
The [CoRE Stack](https://core-stack.org/) (Commoning for Resilience and Equality) is a community-based digital public infrastructure of pre-computed geospatial datasets, analytic pipelines, and user-facing tools that help rural communities, researchers, and practitioners understand and act on socio-ecological challenges. The data spans novel geospatial layers on changes over the years in cropping intensity, water-table levels, health of waterbodies, forests and plantations, and welfare fund allocation, among others, sourced from multiple contributors or built using open ML models operating on satellite data. Rich analytics are computed on this data to build diverse social-ecological indicators through scientifically validated monitoring and modelling methodologies and algorithms. 

What the CoRE stack simplifies for researchers and developers is that it provides ready-to-use pre-computed data of various landscape entities - micro-watersheds, waterbodies, agroforestry plantations - organized in nested and connected spatial units. You do not need to worry about running complex geospatial workflows to generate all this data - we have done all that for you. You can rather just focus on asking the right questions and testing useful hypotheses to draw meaningful insights from the data. 

This challenge asks teams to use the CoRE Stack APIs and datasets to: 1) answer scientific or operational questions about landscapes; 2) build meaningful additions or integrations for the stack; and 3) deliver open-source code, reproducible notebooks, and demonstrations that can be adopted by practitioners or integrated into the CoRE stack.

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
> _Note: Participants should first carefully examine the data and build an intuition for the CoRE stack approach, and then begin solving._

### Bring boundary flexibility
1. CoRE stack provides analytics at the micro-watershed level by default but users often want the same stats at the village level computed from the underlying rasters. For a given village polygon, using the underlying LULC rasters over the years, compute and visualize how cropping intensity (single/double/triple) has changed over the years. Provide stacked-area or stacked-bar charts and a downloadable CSV with annual areas.
2. For the same village, compute seasonal surface-water availability (perennial, winter, monsoon) across years and show trends in area (hectares) per season. 
3. Identify areas within a village that lost tree cover over a chosen period and estimate hectares of degraded land. 

More generally, combine all of the above and describe the most important changes the village has seen over the years and stitch it into a data-story. 

### Problems needing composition of multiple layers
4. Rank micro-watersheds in a tehsil by sensitivity of cropping to drought (compare cropping intensity during identified drought years vs non-drought years to find the micro-watersheds most sensitive to drought).
5. Find the top 5 micro-watersheds most similar to a reference micro-watershed based on terrain, drought frequency, and land-use (use a distance function or propensity score matching methods to identify counterfactual micro-watersheds). Compare their water availability and cropping intensity and water balance indicators. Use this to identify micro-watersheds that are the positive deviants in their tehsil and are worth investigating further on what practices the local communities could be using to be in a better social-ecological position. 
6. For each micro-watershed in a tehsil, position them in a four-quadrant classification using mean cropping intensity (high/low) and mean runoff (high/low) of the micro-watersheds. This can potentially help identify priority micro-watersheds for sustainable cropping intensification: Areas with low cropping intensity but significant surplus runoff could benefit from rainwater harvesting structures to conserve water during the monsoons and utilize it for a second crop later. 
7. In a tehsil, compare % SC/ST population of a village against the number/volume of NREGA works done in the village, and visualize potential marginalized villages where NREGA is not very actively used but can benefit the communities substantially.

### Problems requiring integration with external data
8. Using an external dataset of market locations (e.g., APMC mandi points), analyze whether proximity to markets correlates with cropping intensity and produce visualizations and statistical tests.
9. The CoRE stack itself provides many other datasets that have not been integrated into APIs as yet. For example, the connectivity graph of micro-watersheds (which micro-watersheds drain into which one) and stream-ordering of areas can be analyzed against deforestation occurrences to understand if deforestation is happening more in low-lying areas that are suitable for agriculture or in upland areas where trees might be cut for logging.
10. On similar lines, recent global datasets released on identifying natural forests can be used to study the extent of deforestation or degradation within natural forest ecosystems or outside.  
 
---

## 6. Resources & starting points
- First start with understanding the data in the CoRE stack. The easiest is to check out the Know Your Landscape dashboard: https://www.explorer.core-stack.org/
- And the pan-India [Google Earth Engine app](https://ee-corestackdev.projects.earthengine.app/view/core-stack-gee-app)
- You can also take a look at the CoRE Stack technical manual (datasets & methods) to understand the methodologies that have been used: https://core-stack.org/core-stack-technical-manual-v2/
- Then take a look at the Dataset APIs: https://api-doc.core-stack.org/
- To use the APIs, you will need to register and generate an API key: https://core-stack.org/use-apis/
- A few example notebooks (API + plotting examples) have been provided:
  - Rainfall/ET/runoff water-balance notebook ([Colab](https://colab.research.google.com/drive/1uZH1KZFbe0TUIgCECOz_2cQ1jUfZglsA?usp=sharing))
  - Cropping intensity by micro-watershed notebook ([Colab](https://colab.research.google.com/drive/1zv9TWdzfaEanE_i1kKw2Cr2snoCEhuIg?usp=sharing))
- STAC specs organized by state/district/tehsil also provide relevant metadata: https://stac.core-stack.org/

The very first API you should try is XXX to get the list of locations for which CoRE stack data is already populated. You can begin with these. If you are specifically interested in a particular tehsil for your analysis then let us know and we will generate the data for you. 

---

## 7. Evaluation criteria
Submissions will be scored on:
1. **Relevance & impact** — Does the work address a clear landscape problem and provide actionable insights for practitioners? (25%)
2. **Technical quality** — Correctness, reproducibility, code quality, and validation of methods. (20%)
3. **Innovation** — Novel use of datasets, creative modeling, or tooling that adds value. (20%)
4. **Usability & presentation** — Clarity of visualizations, interpretability for non-technical stakeholders, and quality of demo. (15%)
5. **Openness & reproducibility** — Public code, clear instructions, permissive license, and data provenance. (10%)
6. **Community value** — Potential to be adopted, extensible, and contributed back to the CoRE open-source ecosystem. Bug reporting is especially encouraged! Plus, if you can contribute to fixing the bugs too! (10%)

---

## 8. Submission guidelines
- Submit via the provided form (XXX) with repo and demo links. Attach `metadata.json` and point to the main notebook(s).
- All code should be open-source; include a LICENSE and clear run instructions. If you cannot publish certain external datasets for legal/privacy reasons, include a small synthetic dataset to let reviewers run the analysis.
- Provide at least one reproducible entrypoint (notebook or script) that downloads and processes CoRE Stack data automatically.

---

## 9. Prizes & support
- Monetary prizes for top 3 projects per track.
- Mentorship sessions with domain experts in the CoRE stack network for top finalists.
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
- A small CLI tool that downloads and clips a set of CoRE stack rasters by an uploaded village polygon and returns annual area statistics (cropping intensity, tree cover loss, water bodies area).
- A mapping widget (static or lightweight web UI) that allows practitioners to toggle layers and export a WhatsApp-friendly slide summarizing the last 5 years of trends for a village.
- Scan recent emails on the [CoRE stack googlegroup](https://groups.google.com/g/core-stack-nrm/) for many more ideas and join the group to discuss more!

---
