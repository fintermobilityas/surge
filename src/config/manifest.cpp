#include "config/manifest.hpp"

#include <fmt/format.h>
#include <fstream>
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

namespace surge::config {

namespace {

// Helper: read a YAML node by trying camelCase first, then snake_case.
template <typename T>
T yaml_get(const YAML::Node& node, const std::string& camel_key, const std::string& snake_key, const T& default_val) {
    if (node[camel_key])
        return node[camel_key].as<T>(default_val);
    if (node[snake_key])
        return node[snake_key].as<T>(default_val);
    return default_val;
}

// Helper: return the first present YAML node for the given keys.
YAML::Node yaml_node(const YAML::Node& parent, const std::string& camel_key, const std::string& snake_key) {
    if (parent[camel_key])
        return parent[camel_key];
    return parent[snake_key];
}

TargetConfig parse_target(const YAML::Node& node) {
    TargetConfig target;
    if (!node)
        return target;

    target.os = node["os"].as<std::string>("");
    target.rid = node["rid"].as<std::string>("");

    // Accept both camelCase (persistentAssets) and snake_case (persistent_assets)
    auto assets = yaml_node(node, "persistentAssets", "persistent_assets");
    if (assets) {
        for (const auto& a : assets) {
            target.persistent_assets.push_back(a.as<std::string>());
        }
    }

    if (auto env = node["environment"]) {
        for (auto it = env.begin(); it != env.end(); ++it) {
            target.environment[it->first.as<std::string>()] = it->second.as<std::string>();
        }
    }

    return target;
}

MetadataConfig parse_metadata(const YAML::Node& node) {
    MetadataConfig meta;
    if (!node)
        return meta;
    meta.description = node["description"].as<std::string>("");
    meta.authors = node["authors"].as<std::string>("");
    return meta;
}

AppConfig parse_app(const YAML::Node& node) {
    AppConfig app;
    app.id = node["id"].as<std::string>("");
    app.main = node["main"].as<std::string>("");
    // Accept both camelCase (supervisorId) and snake_case (supervisor_id)
    app.supervisor_id = yaml_get<std::string>(node, "supervisorId", "supervisor_id", "");
    // Accept both camelCase (installDirectory) and snake_case (install_directory)
    app.install_directory = yaml_get<std::string>(node, "installDirectory", "install_directory", "");

    if (auto channels = node["channels"]) {
        for (const auto& ch : channels) {
            app.channels.push_back(ch.as<std::string>());
        }
    }

    app.target = parse_target(node["target"]);
    app.metadata = parse_metadata(node["metadata"]);

    return app;
}

void emit_target(YAML::Emitter& out, const TargetConfig& target) {
    out << YAML::BeginMap;
    out << YAML::Key << "os" << YAML::Value << target.os;
    out << YAML::Key << "rid" << YAML::Value << target.rid;

    if (!target.persistent_assets.empty()) {
        out << YAML::Key << "persistent_assets" << YAML::Value << YAML::BeginSeq;
        for (const auto& a : target.persistent_assets) {
            out << a;
        }
        out << YAML::EndSeq;
    }

    if (!target.environment.empty()) {
        out << YAML::Key << "environment" << YAML::Value << YAML::BeginMap;
        for (const auto& [k, v] : target.environment) {
            out << YAML::Key << k << YAML::Value << v;
        }
        out << YAML::EndMap;
    }

    out << YAML::EndMap;
}

void emit_app(YAML::Emitter& out, const AppConfig& app) {
    out << YAML::BeginMap;
    out << YAML::Key << "id" << YAML::Value << app.id;
    out << YAML::Key << "main" << YAML::Value << app.main;

    if (!app.supervisor_id.empty()) {
        out << YAML::Key << "supervisor_id" << YAML::Value << app.supervisor_id;
    }
    if (!app.install_directory.empty()) {
        out << YAML::Key << "install_directory" << YAML::Value << app.install_directory;
    }

    if (!app.channels.empty()) {
        out << YAML::Key << "channels" << YAML::Value << YAML::BeginSeq;
        for (const auto& ch : app.channels) {
            out << ch;
        }
        out << YAML::EndSeq;
    }

    out << YAML::Key << "target" << YAML::Value;
    emit_target(out, app.target);

    if (!app.metadata.description.empty() || !app.metadata.authors.empty()) {
        out << YAML::Key << "metadata" << YAML::Value << YAML::BeginMap;
        if (!app.metadata.description.empty()) {
            out << YAML::Key << "description" << YAML::Value << app.metadata.description;
        }
        if (!app.metadata.authors.empty()) {
            out << YAML::Key << "authors" << YAML::Value << app.metadata.authors;
        }
        out << YAML::EndMap;
    }

    out << YAML::EndMap;
}

}  // anonymous namespace

SurgeManifest parse_manifest(const std::filesystem::path& path) {
    spdlog::debug("Parsing manifest from: {}", path.string());

    YAML::Node root;
    try {
        root = YAML::LoadFile(path.string());
    } catch (const YAML::Exception& e) {
        throw std::runtime_error(fmt::format("Failed to parse manifest YAML '{}': {}", path.string(), e.what()));
    }

    if (!root.IsMap()) {
        throw std::runtime_error(fmt::format("Manifest '{}' is not a valid YAML mapping", path.string()));
    }

    SurgeManifest manifest;

    // Schema version
    manifest.schema = root["schema"].as<int>(1);

    // Generic section
    if (auto generic = root["generic"]) {
        manifest.generic.token = generic["token"].as<std::string>("");
        manifest.generic.artifacts = generic["artifacts"].as<std::string>("");
        manifest.generic.packages = generic["packages"].as<std::string>("");
    }

    // Storage section
    if (auto storage = root["storage"]) {
        manifest.storage.provider = storage["provider"].as<std::string>("");
        manifest.storage.bucket = storage["bucket"].as<std::string>("");
        manifest.storage.region = storage["region"].as<std::string>("");
        manifest.storage.prefix = storage["prefix"].as<std::string>("");
        manifest.storage.endpoint = storage["endpoint"].as<std::string>("");
    }

    // Lock section
    if (auto lock = root["lock"]) {
        manifest.lock.server = lock["server"].as<std::string>("");
    }

    // Channels
    if (auto channels = root["channels"]) {
        for (const auto& ch : channels) {
            ChannelConfig cc;
            cc.name = ch["name"].as<std::string>("");
            manifest.channels.push_back(std::move(cc));
        }
    }

    // Apps
    if (auto apps = root["apps"]) {
        for (const auto& app_node : apps) {
            manifest.apps.push_back(parse_app(app_node));
        }
    }

    spdlog::debug("Parsed manifest: schema={}, {} channels, {} apps", manifest.schema, manifest.channels.size(),
                  manifest.apps.size());

    return manifest;
}

void write_manifest(const SurgeManifest& manifest, const std::filesystem::path& path) {
    spdlog::debug("Writing manifest to: {}", path.string());

    YAML::Emitter out;
    out << YAML::BeginMap;

    // Schema
    out << YAML::Key << "schema" << YAML::Value << manifest.schema;

    // Generic
    out << YAML::Key << "generic" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "token" << YAML::Value << manifest.generic.token;
    if (!manifest.generic.artifacts.empty()) {
        out << YAML::Key << "artifacts" << YAML::Value << manifest.generic.artifacts;
    }
    if (!manifest.generic.packages.empty()) {
        out << YAML::Key << "packages" << YAML::Value << manifest.generic.packages;
    }
    out << YAML::EndMap;

    // Storage
    out << YAML::Key << "storage" << YAML::Value << YAML::BeginMap;
    out << YAML::Key << "provider" << YAML::Value << manifest.storage.provider;
    if (!manifest.storage.bucket.empty()) {
        out << YAML::Key << "bucket" << YAML::Value << manifest.storage.bucket;
    }
    if (!manifest.storage.region.empty()) {
        out << YAML::Key << "region" << YAML::Value << manifest.storage.region;
    }
    if (!manifest.storage.prefix.empty()) {
        out << YAML::Key << "prefix" << YAML::Value << manifest.storage.prefix;
    }
    if (!manifest.storage.endpoint.empty()) {
        out << YAML::Key << "endpoint" << YAML::Value << manifest.storage.endpoint;
    }
    out << YAML::EndMap;

    // Lock
    if (!manifest.lock.server.empty()) {
        out << YAML::Key << "lock" << YAML::Value << YAML::BeginMap;
        out << YAML::Key << "server" << YAML::Value << manifest.lock.server;
        out << YAML::EndMap;
    }

    // Channels
    out << YAML::Key << "channels" << YAML::Value << YAML::BeginSeq;
    for (const auto& ch : manifest.channels) {
        out << YAML::BeginMap;
        out << YAML::Key << "name" << YAML::Value << ch.name;
        out << YAML::EndMap;
    }
    out << YAML::EndSeq;

    // Apps
    out << YAML::Key << "apps" << YAML::Value << YAML::BeginSeq;
    for (const auto& app : manifest.apps) {
        emit_app(out, app);
    }
    out << YAML::EndSeq;

    out << YAML::EndMap;

    std::ofstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error(fmt::format("Failed to open manifest file for writing: '{}'", path.string()));
    }
    file << out.c_str();
    file.close();

    if (file.fail()) {
        throw std::runtime_error(fmt::format("Failed to write manifest file: '{}'", path.string()));
    }

    spdlog::info("Manifest written to: {}", path.string());
}

std::vector<std::string> validate_manifest(const SurgeManifest& manifest) {
    std::vector<std::string> errors;

    // Schema version
    if (manifest.schema != 1) {
        errors.push_back(fmt::format("Unsupported schema version: {} (expected 1)", manifest.schema));
    }

    // Generic section — token is optional (e.g. filesystem provider needs no auth)

    // Storage
    if (manifest.storage.provider.empty()) {
        errors.push_back("storage.provider is required");
    } else {
        const auto& p = manifest.storage.provider;
        if (p != "s3" && p != "azure_blob" && p != "gcs" && p != "filesystem") {
            errors.push_back(fmt::format("storage.provider '{}' must be one of: s3, azure_blob, gcs, filesystem", p));
        }
    }

    // Channels
    if (manifest.channels.empty()) {
        errors.push_back("At least one channel is required");
    }
    for (size_t i = 0; i < manifest.channels.size(); ++i) {
        if (manifest.channels[i].name.empty()) {
            errors.push_back(fmt::format("channels[{}].name is required", i));
        }
    }

    // Apps
    if (manifest.apps.empty()) {
        errors.push_back("At least one app is required");
    }
    for (size_t i = 0; i < manifest.apps.size(); ++i) {
        const auto& app = manifest.apps[i];
        if (app.id.empty()) {
            errors.push_back(fmt::format("apps[{}].id is required", i));
        }
        if (app.main.empty()) {
            errors.push_back(fmt::format("apps[{}].main is required", i));
        }
        if (app.target.os.empty()) {
            errors.push_back(fmt::format("apps[{}].target.os is required", i));
        }
        if (app.target.rid.empty()) {
            errors.push_back(fmt::format("apps[{}].target.rid is required", i));
        }
    }

    return errors;
}

}  // namespace surge::config
