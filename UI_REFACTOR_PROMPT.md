You are a senior frontend engineer and product designer working on this production project.

Project context:
- Stack: static HTML/CSS/vanilla JS pages served by `local_dashboard.py`.
- Primary screens: `dashboard.html` (list/filter) and `article.html` (detail).
- Data and events are already wired through element IDs and class-based JS.
- Users are Korean-speaking operators monitoring policy/finance press updates.

Goal:
Refactor only UI structure, spacing, typography, and product copy so the service feels like a mature B2B SaaS dashboard built by an experienced team.

Hard constraints:
1. Keep all existing functionality and API behavior.
2. Do not break existing element IDs used by JS (`q`, `organization`, `pressType`, `fromDate`, `sort`, `searchBtn`, `resetBtn`, `tbody`, etc.).
3. Do not change backend contracts or query parameter names.
4. Do not introduce new frameworks or build steps.
5. Do not add unnecessary components or decorative widgets.

Design direction:
- Calm, professional, navy-based product tone (not flashy gradients).
- Clear hierarchy: page summary -> controls -> data table/content.
- Natural spacing rhythm (not perfectly symmetric blocks).
- Fewer heavy boxes/borders; rely on whitespace and subtle separators.
- Practical SaaS patterns: sticky table header, clear filter actions, readable metadata, strong content readability.
- Korean product copy should feel operational and concise.

Specific UI improvements to apply:
1. Dashboard
- Make header copy product-like and concise.
- Group filters logically and keep action buttons visually secondary/primary.
- Reduce visual noise in table container and filter block.
- Improve table readability and column rhythm.
- Keep pagination and total count clear but unobtrusive.

2. Article detail
- Make header and metadata feel editorial and readable.
- Standardize typography and line-height for mixed HTML/plain text body.
- Keep attachment list simple and scannable.
- Use practical labels that real operators expect.

Output requirements:
- Return full updated code for changed files.
- Include a short change summary with rationale by section.
