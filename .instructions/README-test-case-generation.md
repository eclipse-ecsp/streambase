# AI Unit Test Generation Assistant for TestNG / JUnit

This module provides automated instructions and workflows for generating, extending, and validating Java and Rust unit
tests using Copilot Agent.
The instructions are designed to maximize test coverage, ensure correctness, and enforce best practices for CI
environments (Maven/Gradle for Java, Cargo for Rust).

### copilot-instructions-java.md

- Guides Copilot Agent to generate or extend Java unit tests using JUnit or TestNG.
- Enforces high coverage, robust assertions, and proper use of mocking (Mockito).
- Ensures all tests pass, coverage is near 100% before completion.

### copilot-instructions-rust.md

- Directs Copilot Agent to create or improve Rust unit tests.
- Focuses on coverage, edge cases, and correct use of Rust testing features.
- Automates running and validation of tests using Cargo.

## Key Features

- **Self-sufficient automation:** Copilot Agent resolves all setup and dependency issues automatically.
- **CI compatibility:** All generated tests are runnable in standard CI pipelines.
- **Best practices:** Instructions enforce naming conventions, mocking, and efficient setup.

## How to use

- Use Claude Sonnet 3.5 AI model.
- To run the Java Unit Test generation, drag and drop the `copilot-instruction.md` file Java or Rust and the target
  class file into the Agent session of chat Copilot, then ask to apply the instruction.
- The agent will ask you to "Accept All/ Discard All" for adding lines, "Add" for adding new files,
  "Continue" to keep running tests, and it will notify you when the process is complete.
- For any problem you can always delete the conversion history and strat a new agent session.

## Output

- A complete, valid unit test class with high coverage and robust assertions.
- Notification upon successful completion.

