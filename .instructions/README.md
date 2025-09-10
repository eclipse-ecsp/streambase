# AI Assistant Instructions

This directory contains comprehensive instructions and guidelines for AI assistants working on this project. These files provide context, standards, and best practices to ensure consistent code generation, testing, and review processes.

## Overview

This instruction set is designed to work with AI code generation tools while maintaining strict adherence to established coding standards and modern Java development practices.

## Files Overview

### Core Instruction Files
- **`java-coding-guidelines.md`** - Comprehensive Java coding standards based on project's checkstyle configuration, integrated with modern Java best practices and static analysis recommendations
- **`test-generation.md`** - Specialized instructions for generating unit tests using TestNG or JUnit with high coverage and CI compatibility  
- **`code-review.md`** - Detailed code review checklist and standards with color-coded severity levels for systematic review processes

## Checkstyle Integration

The guidelines in these instruction files are directly based on and aligned with the **`checkstyle.xml`** configuration file. This ensures that:

- ✅ **All generated code passes Checkstyle validation**
- ✅ **Consistent formatting and style across the codebase**
- ✅ **Automated quality enforcement through CI/CD pipelines**
- ✅ **Reduced code review overhead**

### Key Checkstyle Rules Covered

The instruction files incorporate all major Checkstyle rules including:

| Category | Rules Covered |
|----------|---------------|
| **Formatting** | Line length (120 chars), indentation (4 spaces), brace placement, whitespace rules |
| **Naming** | Package names, class names (PascalCase), methods (camelCase), variables, constants |
| **Structure** | Import organization, modifier order, method length (≤60 lines), cyclomatic complexity (≤15) |
| **Documentation** | Javadoc requirements, tag ordering, annotation placement |
| **Quality** | No star imports, proper exception handling, utility class constructors |

## Modern Java Best Practices

Beyond Checkstyle compliance, the guidelines include modern Java development practices:

### 🚀 **Language Features**
- **Records** for data classes and DTOs
- **Pattern matching** for instanceof and switch expressions
- **Stream API** and lambda expressions for collection processing
- **Optional** for null-safe programming

### 🔍 **Static Analysis Integration**
- **SonarQube rules** for bug pattern prevention
- **Code smell detection** and prevention strategies
- **Security vulnerability** awareness

### 🏗️ **Architecture Patterns**
- **Immutability** by default
- **Builder pattern** for complex objects
- **Method references** over lambda expressions where appropriate

## Usage Instructions

### For Code Generation
Reference `java-coding-guidelines.md` when:
- Generating new Java classes, methods, or code blocks
- Modifying existing code to ensure compliance
- Applying modern Java best practices
- Integrating static analysis tool recommendations

### For Unit Test Generation
Reference `test-generation.md` when:
- Creating new unit test classes
- Extending existing test suites
- Ensuring high test coverage (near 100%)
- Generating tests compatible with CI/CD pipelines
- Using TestNG or JUnit frameworks with proper mocking (Mockito)

### For Code Review
Reference `code-review.md` when:
- Conducting systematic code reviews
- Identifying critical vs moderate issues
- Ensuring code quality and maintainability
- Following consistent review standards

## Key Features

- **Self-sufficient automation**: AI assistants can resolve setup and dependency issues automatically
- **CI compatibility**: All generated code and tests are runnable in standard CI pipelines  
- **Best practices enforcement**: Instructions ensure naming conventions, proper mocking, and efficient setup
- **Comprehensive coverage**: Guidelines cover formatting, structure, documentation, and quality aspects

## Integration with Development Workflow

These instructions are designed to be used by AI assistants during:
1. **Code Generation** - Following coding standards and modern practices
2. **Test Creation** - Generating comprehensive, maintainable test suites
3. **Code Review** - Applying consistent review criteria
4. **Refactoring** - Maintaining quality while improving code structure
5. **CI/CD Integration** - Ensuring all output passes automated checks

## Project Context

This project uses:
- **Java** with Maven build system
- **Checkstyle** for code quality enforcement
- **TestNG/JUnit** for testing frameworks
- **Mockito** for mocking in tests
- **Static Analysis** tools (SonarQube, PMD) for quality assurance

## Maintenance

When project standards change:
1. Update the relevant instruction files
2. Ensure consistency across all instruction files
3. Update this README if new instruction types are added
4. Verify integration with checkstyle.xml configuration

For human developers, refer to the main project README.md in the root directory.
