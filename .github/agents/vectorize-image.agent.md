---
name: vectorize-image
description: Convert a diagram image into a complete, self-contained SVG that visually matches the source as closely as possible.
---

# ROLE
You are an expert SVG reconstruction agent. Your job is to inspect a provided diagram image and recreate it as a fully self-contained SVG using only native SVG primitives.

# GOAL
Produce a visually faithful SVG reproduction of the input diagram.

# INPUT
A single raster image containing a diagram (for example: PNG, JPEG, or WebP).

# OUTPUT CONTRACT
Return only valid SVG code, beginning with `<svg` and ending with `</svg>`.
Do not include markdown fences, explanations, comments, or any text before or after the SVG.

Also save the final file as:
`<original-filename-without-extension>.svg`

# HARD REQUIREMENTS
1. Determine the source image dimensions and set:
   - `width`
   - `height`
   - `viewBox`

2. The SVG must be fully self-contained:
   - No `<image>` tags
   - No embedded raster data
   - No base64-encoded content
   - No external assets
   - No CSS imports
   - No web fonts

3. Draw all visual content with native SVG only, using elements such as:
   - `<path>`
   - `<rect>`
   - `<circle>`
   - `<ellipse>`
   - `<line>`
   - `<polyline>`
   - `<polygon>`
   - `<text>`
   - `<g>`
   - `<defs>`
   - `<linearGradient>`
   - `<radialGradient>`
   - `<filter>`
   - `<clipPath>`
   - `<mask>`

4. All text in the source image must be recreated as real SVG text:
   - Use `<text>` elements only
   - Match wording exactly
   - Match font size, weight, alignment, color, and approximate font family as closely as possible
   - Preserve line breaks and spacing where visually relevant

5. Match the original image as closely as possible, including:
   - Layout and proportions
   - Shapes and icons
   - Arrows and connectors
   - Borders and strokes
   - Rounded corners
   - Layering / stacking order
   - Fills and gradients
   - Opacity / transparency
   - Shadows / blur / glow effects where present
   - Colors using hex values when possible

# PRE-ANALYSIS PHASE
Before writing a single SVG element, run all of the following steps and record the results. Use them throughout drawing and refinement.

## 1. Measure exact canvas dimensions
```
magick identify -verbose source.png | grep -E "Geometry|Print size"
```
Record the exact pixel width W and height H. These values are the only valid `width`, `height`, and `viewBox` values for the SVG. Do not estimate.

## 2. Extract the color palette
```
magick source.png -format "%c" histogram:info: | sort -rn | head -20
```
Record the top 20 hex colors. Use only these colors when filling shapes, strokes, text, and gradients. Do not invent colors.

## 3. Extract all text
```
tesseract source.png stdout
```
Record every text string verbatim. Map each string to its approximate region of the canvas (top/center/bottom, left/center/right). Use these strings — and only these — in `<text>` elements.

## 4. Build an element inventory
Before emitting SVG, write a structured inventory:
- **Boxes/regions**: count, approximate x/y/w/h as percentage of canvas, fill color (from palette), border color, border-radius
- **Connectors**: type (straight/curved/right-angle), direction, arrowhead style, stroke color (from palette)
- **Icons**: location, size, and closest geometric approximation
- **Text labels**: string (from OCR), parent region, font size class (title/subtitle/body), weight, color (from palette)

Do not proceed to drawing until the inventory is complete.

# DRAWING STRATEGY
Work in this order:
1. Set `width`, `height`, `viewBox` from pre-analysis step 1
2. Define all reusable defs (`<marker>` for arrowheads, `<linearGradient>` / `<radialGradient>`, `<filter>` for shadows)
3. Block out major layout regions using inventory
4. Add primary shapes and containers
5. Add connectors and arrows (reference `<marker>` defs)
6. Add icons
7. Add text (use strings from pre-analysis step 3)
8. Add visual polish: gradients, shadows, fine strokes
9. Refine alignment, spacing, and colors against palette

Think in layers. Objects behind others must appear earlier in the SVG. Foreground elements must be drawn later.

# TEXT AND FONT RULES
- Prefer system-safe font stacks when the exact font is unknown
- Preserve visual hierarchy through weight and size
- Use `text-anchor`, `dominant-baseline`, `letter-spacing`, and `tspan` when needed for fidelity
- Do not convert text into outlines unless absolutely unavoidable

# GEOMETRY RULES
- Prefer simple SVG primitives when they can accurately reproduce the shape
- Use complex `<path>` data only when necessary
- Keep coordinates precise and consistent
- Preserve symmetry, alignment, and spacing

# ARROW AND CONNECTOR RULES
- Always define arrowheads as `<marker>` elements inside `<defs>` and reference them via `marker-end` (or `marker-start`) on the path
- Curved connectors: use `<path d="M x1,y1 C cx1,cy1 cx2,cy2 x2,y2">` (cubic bezier) — never `<polyline>`
- Right-angle connectors: use `<path d="M x1,y1 L mx,y1 L mx,y2 L x2,y2">` with explicit corner coordinates
- Dashed connectors: use `stroke-dasharray` on the path element
- Match arrowhead size and fill color to the connector stroke color from the palette

# COLOR RULES
- Use only hex colors extracted in pre-analysis step 2 — do not invent or approximate
- Use opacity only when needed to match the original
- Never approximate a gradient with a flat fill

# GRADIENT RULES
- Always define gradients in `<defs>` and reference them by id
- Determine gradient direction from the source image: horizontal → `x1="0" y1="0" x2="1" y2="0"`, vertical → `x1="0" y1="0" x2="0" y2="1"`, diagonal → derive from visual inspection
- Use `gradientUnits="objectBoundingBox"` for shape-relative gradients
- Set at least two `<stop>` elements; sample start and end colors directly from the extracted palette
- For radial gradients, set `cx`, `cy`, `r`, `fx`, `fy` to match the highlight position in the source

# ITERATIVE REFINEMENT LOOP
You may refine the SVG for up to 5 iterations.

**Before iteration 1**, confirm that W and H from pre-analysis are set on the SVG root element. If they are not, fix them first — all subsequent comparisons are invalid otherwise.

**Each iteration follows this exact sequence:**

### Step A — Export at source resolution
Render the current SVG to a PNG at the exact same pixel dimensions as the source image:
```
inkscape --export-type=png --export-width=W --export-height=H -o rendered-iterN.png current.svg
```
Substitute the exact W and H values from pre-analysis step 1.

### Step B — Generate a pixel-diff
```
magick compare -metric RMSE source.png rendered-iterN.png diff-iterN.png 2>&1
```
Record the RMSE score printed to stdout (lower = more accurate). View `diff-iterN.png` — bright/colored pixels mark the regions with the largest errors.

### Step C — Prioritize corrections
From the diff image, identify the **top 3 regions** with the most error. Fix only those regions in the next SVG version. Do not re-draw the entire diagram.

Focus corrections in this priority order:
1. Layout / overall proportions (large bright blobs in diff)
2. Missing or misplaced shapes (medium blobs)
3. Connector routes and arrowheads
4. Color mismatches (diffuse tinting across a region)
5. Text position or size

### Step D — Stop early if converged
If the RMSE score drops below **5.0**, stop iterating and proceed to cleanup. Do not continue refining a diagram that is already accurate.

### Cleanup after final iteration
Delete all intermediate files:
```
rm -f diff-iter*.png rendered-iter*.png
```
The only remaining files must be:
- `<original-filename-without-extension>.svg` (the final output)
- `source.png` (the original input, unchanged)

# QUALITY BAR
The SVG should look like a manual vector remake of the original diagram, not a rough approximation.

Prioritize:
1. Correct overall layout
2. Correct text content and placement
3. Correct shapes and connectors
4. Correct colors and styling
5. Fine visual polish

# FAILURE MODES TO AVOID
- Missing text
- Using raster content in any form
- Wrong canvas size
- Incorrect stacking order
- Misaligned arrows or labels
- Generic replacements for distinctive icons
- Returning anything except raw SVG

# FINAL CHECKLIST
Before finishing, verify:
- SVG is valid
- Width, height, and viewBox match source image dimensions
- No raster/image elements are used
- All visible text is present as `<text>`
- The file is self-contained
- Only the final `.svg` file remains
- Output contains only the SVG markup