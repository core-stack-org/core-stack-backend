# Overview of CoRE Stack Backend Docs

Welcome to the CoRE Stack Backend documentation. This comprehensive guide provides everything you need to understand, develop, and maintain the Natural Resource Management (NRM) application backend.

## What is CoRE Stack Backend?

The CoRE Stack Backend is a Django-based REST API that powers natural resource management applications, specifically designed for:

- **Plantation Management**: Upload and manage KML files, track plantation boundaries, and monitor forest resources
- **Watershed Planning**: Create and manage watershed plans, track resources, and monitor project progress
- **Multi-tenant Organization Management**: Support multiple organizations with proper data isolation and role-based access control

## System Architecture

The backend follows a modern, scalable architecture:

- **Framework**: Django 4.x with Django REST Framework
- **Authentication**: JWT-based authentication with refresh tokens
- **Database**: PostgreSQL with spatial data support (PostGIS)
- **Authorization**: Hierarchical role-based access control system
- **File Management**: Secure file upload and processing for geospatial data

## Key Features

### **Multi-Organization Support**
- Isolated data per organization
- Organization-level user management
- Flexible project assignment within organizations

### **Advanced Permission System**
- **Superadmin**: Full system access across all organizations
- **Organization Admin**: Full control within their organization
- **Project Manager**: Manage specific projects and their users
- **App User**: Role-based access to specific features

### **Plantation Management**
- KML file upload and validation
- Automatic GeoJSON conversion
- Duplicate file detection using SHA-256 hashing
- Spatial data visualization support

### **Watershed Planning**
- Comprehensive plan creation and management
- Geographic hierarchy support (State → District → Block)
- DPR (Detailed Project Report) workflow management
- Multi-level plan access (Global, Organization, Project levels)

### **Developer-Friendly API**
- RESTful API design
- Comprehensive filtering and search capabilities
- Pagination support for large datasets
- Detailed error handling and validation

## Documentation Structure

This documentation is organized into several key sections:

### **API Documentation** (`/guide/api/`)
- **[API Reference](guide/api/api.md)**: Complete API endpoint documentation with examples
- **[User Hierarchy](guide/api/hierarchy.md)**: User roles and permission system explained

### **Design Documentation** (`/guide/design/`)
- **[Database Design](guide/design/db_design.md)**: Database schema and relationships
- **[Frontend Guide](guide/design/frontend.md)**: Guidelines for frontend implementation

### **Development Guidelines** (`/guide/development/`)
- **[Python Practices](guide/development/practices.md)**: Comprehensive coding standards and best practices

## Quick Start

### For API Consumers
1. Review the [API Documentation](guide/api/api.md) for available endpoints
2. Understand the [User Hierarchy](guide/api/hierarchy.md) system
3. Implement authentication flow using JWT tokens

### For Developers
1. Study the [Database Design](guide/design/db_design.md) to understand data relationships
2. Follow [Python Practices](guide/development/practices.md) for code quality
3. Review the [Frontend Guide](guide/design/frontend.md) for client-side integration

### For System Administrators
1. Understand the user hierarchy for proper role assignment
2. Review organization and project management workflows
3. Set up proper authentication and authorization flows

## Technology Stack

- **Backend Framework**: Django 4.x
- **API Framework**: Django REST Framework
- **Database**: PostgreSQL with PostGIS
- **Authentication**: JWT (JSON Web Tokens)
- **File Storage**: Django file handling with hash-based deduplication
- **Geospatial**: KML processing and GeoJSON conversion
- **Testing**: Django TestCase and pytest
- **Code Quality**: Black, isort, flake8, mypy

## Core Concepts

### Organizations
Top-level entities that contain users and projects. Each organization operates independently with its own data isolation.

### Projects
Contain specific applications (plantation or watershed) and manage user access through role-based permissions.

### Applications
- **Plantation App**: Manages KML files and forest boundary data
- **Watershed App**: Handles watershed planning and resource management

### User Roles
Hierarchical permission system ensuring proper access control across all levels of the application.

## Getting Started

To get started with the CoRE Stack Backend:

1. **API Users**: Begin with the [API Documentation](guide/api/api.md)
2. **Developers**: Start with [Python Practices](guide/development/practices.md)
3. **System Architects**: Review [Database Design](guide/design/db_design.md)
4. **Frontend Developers**: Check the [Frontend Guide](guide/design/frontend.md)

## Support and Contributing

This documentation is continuously updated to reflect the latest features and best practices. For questions or suggestions, please refer to the specific documentation sections that address your needs.

---

**Note**: This documentation assumes familiarity with Django, REST APIs, and modern web development practices. If you're new to these technologies, we recommend reviewing the foundational concepts before diving into the specific implementation details.
