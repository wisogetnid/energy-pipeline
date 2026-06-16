---
name: python-tdd
description: Execute the strict Red-Green-Refactor TDD loop using pytest.
---
## What I do
I guide you through the strict Test-Driven Development cycle for Python code. I ensure you prove a requirement is unmet before writing the code to meet it.

## When to use me
Use me whenever you are adding a new feature, fixing a bug, or modifying business logic in Python.

## Instructions
1. **Red (Write Test)**: 
   - Write a single `pytest` test that captures the exact requirement or bug fix.
   - Run the specific test file via bash to confirm it fails. This proves the test is valid and testing the right thing.
2. **Green (Implement)**:
   - Write the *minimum* amount of Python code required to make the failing test pass. Do not over-engineer or add speculative features.
   - Run the test again. Continue tweaking until it passes.
3. **Refactor**:
   - Review the newly written code and test. 
   - Can the code be simplified? Are variables named clearly? 
   - Refactor the code and run `pytest` after every change to ensure tests stay green.
