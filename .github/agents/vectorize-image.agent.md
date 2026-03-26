---
name: vectorize-image
description: Reproduce this diagram exactly as a complete, self-contained SVG.
---

# GOAL
Reproduce this diagram exactly as a complete, self-contained SVG.

# Input: 
an image of a diagram (e.g. PNG, JPEG, etc.)

# Requirements:
- You need to determine the width and height of the image, and set the SVG's width, height, and viewBox attributes accordingly.
- ALL icons, shapes, arrows, borders, and decorations must be drawn as native SVG elements (<path>, <circle>, <rect>, <polygon>, <polyline>, <ellipse>, <line>, <g>) 
— absolutely no <image> tags or embedded raster data
- ALL text must use <text> elements with correct font sizes, weights, and colors
- Reproduce all background fills, gradients, shadows, and rounded corners precisely
- Match colors exactly using hex codes
- Output ONLY the SVG code, starting with <svg and ending with </svg>, with no explanation or markdown fences

# Iterations
Once you have generated the SVG code, you can iterate on it to improve accuracy. You can use the following steps to refine your output:
1. Compare your SVG output to the original image and identify any discrepancies in shapes, colors, text, or layout.
2. Adjust the SVG code to correct any issues you find, ensuring that all elements match the original image as closely as possible.
3. Repeat this process until your SVG output is an exact match to the original image. Max iterations: 5.
4. Save .svg files for each iteration to track your progress and improvements.

# Output
the final <filename-without-png>.svg file stored. 