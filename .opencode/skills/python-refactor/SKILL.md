---
name: python-refactor
description: Safely refactor Python code in small, atomic, test-backed steps.
---
## What I do
I help you break down complex Python refactoring tasks into small, safe, and easily reversible steps while leaning on the test suite for safety.

## When to use me
Use me when cleaning up technical debt, extracting functions/classes, or improving the readability of existing code.

## Instructions
Before starting: Run the relevant `pytest` tests to ensure we are starting from a green state.

1. **Analyze & Plan**: Identify the specific "code smell" (e.g., long function, duplicated code) and list the small steps needed to fix it.
2. **Execute One Step**: Apply *only* the first step of your plan (e.g., extract one helper function).
3. **Verify**: Run the tests. If the tests fail, revert the change immediately using git or by undoing the edit, then plan a smaller step.
4. **Commit/Save**: Once the small step is green, move to the next step.
