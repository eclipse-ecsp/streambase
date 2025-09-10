---
description: 'Unified Java coding guidelines for building Java applications'
applyTo: '**/*.java'
---

# Unified Java Coding Guidelines Instructions

This file contains comprehensive coding standards and guidelines that should be followed when generating or modifying Java code in this project. These guidelines are based on the project's checkstyle configuration, integrated with modern Java best practices and static analysis recommendations.

## General Instructions

- First, prompt the user if they want to integrate static analysis tools (SonarQube, PMD, Checkstyle) into their project setup. If yes, provide guidance on tool selection and configuration.
- If the user declines static analysis tools or wants to proceed without them, continue with implementing the best practices, bug patterns and code smell prevention guidelines outlined below.
- Address code smells proactively during development rather than accumulating technical debt.
- Focus on readability, maintainability, and performance when refactoring identified issues.
- Use IDE / Code editor reported warnings and suggestions to catch common patterns early in development.

## File and Character Encoding
- Use UTF-8 encoding for all files
- No tab characters allowed - use spaces for indentation
- File extensions: `.java`, `.properties`, `.xml`
- Exclude `module-info.java` files from checks

## Line Length and Formatting
- **Maximum line length: 120 characters**
- Ignore line length for package declarations, imports, and URLs
- No line wrapping for package declarations, imports, and static imports

## Indentation and Whitespace
- **Base indentation: 4 spaces**
- **Brace adjustment: 2 spaces**
- **Case indentation: 4 spaces**
- **Line wrapping indentation: 4 spaces**
- **Array initialization indentation: 2 spaces**

### Whitespace Rules
- Add whitespace after: commas, semicolons, typecasts, if/else/while/do/for/finally statements, switch/synchronized/try/catch, lambda expressions
- Add whitespace around operators and braces (except empty blocks which can be `{}`)
- No whitespace before: commas, semicolons, post-increment/decrement, dots, method references
- No whitespace after: method references, dots
- No whitespace inside parentheses for various constructs

## Naming Conventions

### Packages
- **Format**: All lowercase, separated by dots
- **Pattern**: `^[a-z]+(\.[a-z][a-z0-9]*)*$`
- **Example**: `com.example.mypackage`

### Classes, Interfaces, Enums, Records, Annotations
- **Format**: PascalCase (UpperCamelCase)
- **Pattern**: Standard Java naming (uppercase first letter)
- **Example**: `MyClassName`, `UserService`
- Use nouns for classes (`UserService`) and verbs for methods (`getUserById`)
- Avoid abbreviations and Hungarian notation

### Methods
- **Format**: camelCase (lowerCamelCase)
- **Pattern**: `^[a-z][a-z0-9]\w*$`
- **Example**: `getUserData()`, `processRequest()`

### Variables (Members, Parameters, Local Variables)
- **Format**: camelCase (lowerCamelCase)
- **Pattern**: `^[a-z]([a-z0-9][a-zA-Z0-9]*)?$`
- **Example**: `userName`, `userId`, `data`

### Constants
- **Format**: UPPER_SNAKE_CASE
- **Example**: `MAX_SIZE`, `DEFAULT_TIMEOUT`

### Type Parameters (Generics)
- **Format**: Single uppercase letter or ending with 'T'
- **Pattern**: `(^[A-Z][0-9]?)$|([A-Z][a-zA-Z0-9]*[T]$)`
- **Example**: `T`, `E`, `K`, `V`, `ResponseT`

## Modern Java Best Practices

### Records and Data Classes
- **Records**: For classes primarily intended to store data (e.g., DTOs, immutable data structures), **Java Records should be used instead of traditional classes**.

### Pattern Matching and Type Inference
- **Pattern Matching**: Utilize pattern matching for `instanceof` and `switch` expression to simplify conditional logic and type casting.
- **Type Inference**: Use `var` for local variable declarations to improve readability, but only when the type is explicitly clear from the right-hand side of the expression.
- **Note**: Checkstyle configuration prohibits `var` keyword - follow checkstyle rules in strict environments.

### Immutability and Collections
- **Immutability**: Favor immutable objects. Make classes and fields `final` where possible. Use collections from `List.of()`/`Map.of()` for fixed data. Use `Stream.toList()` to create immutable lists.
- **Streams and Lambdas**: Use the Streams API and lambda expressions for collection processing. Employ method references (e.g., `stream.map(Foo::toBar)`).

### Null Handling
- **Null Handling**: Avoid returning or accepting `null`. Use `Optional<T>` for possibly-absent values and `Objects` utility methods like `equals()` and `requireNonNull()`.

## Code Structure and Blocks

### Braces
- **Left brace placement**: Same line (K&R style)
- **Right brace placement**: 
  - Same line for try/catch/finally/if/else/do
  - Separate line for classes, methods, constructors, loops, static/instance init blocks
- **Always use braces** for if, else, while, do, for statements (even single statements)

### Empty Blocks
- Empty blocks must contain text or be represented as `{}`
- Applies to try, finally, if, else, switch blocks

### Class and Method Structure
- **One top-level class per file**
- **One statement per line**
- **No multiple variable declarations on same line**
- **Array type style**: `String[] args` (not `String args[]`)
- **Method length**: Maximum 60 lines (excluding empty lines)
- **Cyclomatic complexity**: Maximum 15

## Import Organization
- **No star imports** (`import java.util.*` not allowed)
- **Import order**:
  1. Third-party packages
  2. Special imports (javax.*)
  3. Standard Java packages (java.*)
  4. Static imports
- Sort imports alphabetically within groups
- No empty lines between groups

## Switch Statements
- **Always include default case**
- **Fall-through requires comment**

## Modifiers
- Use standard modifier order: `public protected private abstract default static final transient volatile synchronized native strictfp`

## Line Separators and Wrapping
- Empty line separation required between:
  - Package declaration and imports
  - Import groups
  - Class/interface/enum definitions
  - Static/instance initialization blocks
  - Method definitions
  - Constructor definitions
  - Variable definitions (with exceptions for fields)

### Separator Wrapping
- **Dots**: New line before dot
- **Commas**: End of line
- **Ellipsis**: End of line
- **Array declarators**: End of line
- **Method references**: New line before `::` 

## Operators
- **Operator wrapping**: New line before operator
- **Applies to**: `&&`, `||`, `+`, `-`, `*`, `/`, `%`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `instanceof`, `?`, method references

## Documentation (Javadoc)

### Required Javadoc
- **Public types**: Protected scope and above require Javadoc
- **Public methods**: Methods with 2+ lines require Javadoc (except @Override and @Test)
- **Constructors**: Follow same rules as methods

### Javadoc Format
- **Tag order for methods**: `@param`, `@return`, `@throws`, `@since`, `@see`, `@deprecated`
- **Tag order for classes**: `@author`, `@since`, `@param`, `@see`, `@deprecated`
- **Tag order for other elements**: `@param`, `@return`, `@throws`, `@deprecated`
- Use proper indentation for tag continuation
- Include summary sentence
- Use proper paragraph formatting
- Require empty line before block tag groups

## Annotations
- **Placement**: Separate line for classes, interfaces, enums, methods, constructors
- **Variables**: Multiple annotations on same line allowed
- **@Override**: Required when overriding methods
- **@Since**: Required for new classes and methods (specify version)

## Code Quality Rules

### Complexity and Length
- **Cyclomatic complexity**: ≤ 15
- **Method length**: ≤ 60 lines
- **Variable declaration usage distance**: Keep variables close to usage

### Prohibited Constructs
- No finalizers
- No utility class public constructors (hide them)
- No abbreviations as words in names (except length 0)
- No magic numbers (except 0 and 1)
- No `var` keyword for local variables (checkstyle restriction)
- No star imports

### Required Practices
- Use `@Override` annotation
- Require `this` for field access (methods excluded)
- Proper exception variable naming in catch blocks
- Handle fall-through in switch statements
- Include default cases in switch statements

## Bug Patterns (SonarQube Rules)

| Rule ID | Description                                                 | Example / Notes                                                                                  |
| ------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `S2095` | Resources should be closed                                  | Use try-with-resources when working with streams, files, sockets, etc.                           |
| `S1698` | Objects should be compared with `.equals()` instead of `==` | Especially important for Strings and boxed primitives.                                           |
| `S1905` | Redundant casts should be removed                           | Clean up unnecessary or unsafe casts.                                                            |
| `S3518` | Conditions should not always evaluate to true or false      | Watch for infinite loops or if-conditions that never change.                                     |
| `S108`  | Unreachable code should be removed                          | Code after `return`, `throw`, etc., must be cleaned up.                                          |

## Code Smells (SonarQube Rules)

| Rule ID | Description                                            | Example / Notes                                                               |
| ------- | ------------------------------------------------------ | ----------------------------------------------------------------------------- |
| `S107`  | Methods should not have too many parameters            | Refactor into helper classes or use builder pattern.                          |
| `S121`  | Duplicated blocks of code should be removed            | Consolidate logic into shared methods.                                        |
| `S138`  | Methods should not be too long                         | Break complex logic into smaller, testable units.                             |
| `S3776` | Cognitive complexity should be reduced                 | Simplify nested logic, extract methods, avoid deep `if` trees.                |
| `S1192` | String literals should not be duplicated               | Replace with constants or enums.                                              |
| `S1854` | Unused assignments should be removed                   | Avoid dead variables—remove or refactor.                                      |
| `S109`  | Magic numbers should be replaced with constants        | Improves readability and maintainability.                                     |
| `S1188` | Catch blocks should not be empty                       | Always log or handle exceptions meaningfully.                                 |

## Comments
- Use proper indentation for single-line and block comments
- Support suppression comments:
  - `CHECKSTYLE.OFF: RuleName` / `CHECKSTYLE.ON: RuleName`
  - `CHECKSTYLE.SUPPRESS: RuleName` (suppresses next line)
- Flag TODO and FIXME comments for review

## Generic Types
- Proper whitespace around generic type parameters
- Follow standard generic naming conventions

## Empty Lines and Separation
- Allow no empty line between fields
- Empty line separation for major code blocks
- Proper separation between different types of declarations

## Build and Verification
- After adding or modifying code, verify the project continues to build successfully.
- If the project uses Maven, run `mvn clean install`.
- If the project uses Gradle, run `./gradlew build` (or `gradlew.bat build` on Windows).
- Ensure all tests pass as part of the build.

## Example Code Structure

```java
package com.example.myproject;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.springframework.stereotype.Service;

/**
 * Service class for handling user operations.
 * 
 * @author John Doe
 * @since 1.0
 */
@Service
public class UserService {
    
    private static final int MAX_USERS = 100;
    
    private final UserRepository userRepository;
    
    /**
     * Constructor for UserService.
     * 
     * @param userRepository the user repository
     * @since 1.0
     */
    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    
    /**
     * Retrieves user data by ID.
     * 
     * @param userId the user identifier
     * @return the user data or null if not found
     * @throws IllegalArgumentException if userId is invalid
     * @since 1.0
     */
    @Nullable
    public UserData getUserById(String userId) {
        if (userId == null || userId.isEmpty()) {
            throw new IllegalArgumentException("User ID cannot be null or empty");
        }
        
        return this.userRepository.findById(userId);
    }
}
```

## Suppression
- Use suppression files when needed: `checkstyle-suppressions.xml`
- Use XPath suppressions for complex cases
- Support `@SuppressWarnings` annotations
- Document reasons for suppressions

## Summary

These unified guidelines ensure consistent, readable, and maintainable Java code across the project by combining:

1. **Checkstyle Configuration Standards** (primary authority)
2. **Modern Java Best Practices** (Records, Pattern Matching, Streams)
3. **Static Analysis Rules** (SonarQube bug patterns and code smells)
4. **Build Integration** (Maven/Gradle verification)

Always apply these standards when generating or modifying code. In case of conflicts between modern practices and checkstyle rules, follow the checkstyle configuration (e.g., avoid `var` when checkstyle prohibits it).
