"""Pydantic request models for the source-images API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Base64File(BaseModel):
    """A file with its content base64-encoded."""

    filename: str = Field(description="Relative path within its logical directory")
    content_base64: str = Field(description="Base64-encoded file content")
    mode: int | None = Field(default=None, description="File permission bits (e.g. 0o755)")


class Base64Directory(BaseModel):
    """A directory containing base64-encoded files."""

    name: str = Field(description="Directory name (e.g., 'uperf')")
    files: list[Base64File] = Field(default_factory=list)


class RegistryUrlDetails(BaseModel):
    """URL components for a container registry."""

    dest_image_url: str = Field(alias="dest-image-url")
    source_image_url: str = Field(alias="source-image-url")
    host: str
    project: str | None = None
    label: str

    model_config = {"populate_by_name": True}


class RegistryConfig(BaseModel):
    """Configuration for a single registry (public or private)."""

    url_details: RegistryUrlDetails = Field(alias="url-details")
    tls_verify: bool = Field(default=True, alias="tls-verify")
    quay_expiration_length: str | None = Field(
        default=None, alias="quay-expiration-length"
    )
    quay_refresh_expiration_api_url: str | None = Field(
        default=None, alias="quay-refresh-expiration-api-url"
    )
    quay_refresh_expiration_require_success: bool = Field(
        default=False, alias="quay-refresh-expiration-require-success"
    )

    model_config = {"populate_by_name": True}


class WorkshopConfig(BaseModel):
    """Workshop build tool configuration."""

    workshop_pl_content: str = Field(
        alias="workshop-pl-content",
        description="Base64-encoded workshop.pl script",
    )
    schema_content: str = Field(
        alias="schema-content",
        description="Base64-encoded workshop schema JSON",
    )
    registries_schema_content: str = Field(
        alias="registries-schema-content",
        description="Base64-encoded workshop registries-schema.json",
    )
    force_builds: bool = Field(default=False, alias="force-builds")

    model_config = {"populate_by_name": True}


class SourceImagesRequest(BaseModel):
    """Top-level request body for POST /api/v1/source-images."""

    arch: str = Field(description="Container architecture: x86_64, aarch64, etc.")
    image_ids: dict[str, dict[str, dict]] = Field(
        alias="image-ids",
        description="benchmark → userenv → {} mapping",
    )
    registries: dict[str, RegistryConfig]
    registries_json_content: str = Field(
        alias="registries-json-content",
        description="Base64 of registries.json",
    )
    push_token_content: dict[str, str] | None = Field(
        default=None,
        alias="push-token-content",
        description="registry_type → base64 token content",
    )
    pull_token_content: dict[str, str] | None = Field(
        default=None,
        alias="pull-token-content",
        description="registry_type → base64 token content",
    )
    bench_dirs: dict[str, Base64Directory] = Field(
        alias="bench-dirs",
        description="Benchmark directories with workshop.json etc.",
    )
    utilities: list[str] = Field(default_factory=list)
    utility_dirs: dict[str, Base64Directory] | None = Field(
        default=None, alias="utility-dirs"
    )
    userenv_files: dict[str, str] = Field(
        alias="userenv-files",
        description="userenv_name → base64 of .json file",
    )
    workshop: WorkshopConfig
    rickshaw_files: Base64Directory = Field(
        alias="rickshaw-files",
        description="engine/, userenvs/ content needed for builds",
    )
    roadblock_workshop_json: str = Field(
        alias="roadblock-workshop-json",
        description="Base64 of roadblock workshop.json",
    )
    toolbox_dir: Base64Directory = Field(
        alias="toolbox-dir",
        description="Toolbox files copied into images",
    )
    toolbox_workshop_json: str = Field(
        alias="toolbox-workshop-json",
        description="Base64 of toolbox workshop.json",
    )
    use_workshop: bool = Field(alias="use-workshop")
    quay_refresh_expiration_tokens: dict[str, str] | None = Field(
        default=None, alias="quay-refresh-expiration-tokens"
    )
    log_level: str = Field(default="info", alias="log-level")

    model_config = {"populate_by_name": True}
