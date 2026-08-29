#include "storage/local_storage_manager.h"

#include <array>
#include <stdexcept>
#include <system_error>

#include "storage/index/index_page.h"
#include "storage/page/page.h"

std::string LocalStorageManager::heapFileNameFor(
    const std::string& table_name) const {
  return table_name + ".db";
}

std::string LocalStorageManager::indexFileNameFor(
    const std::string& table_name,
    const std::vector<std::string>& indexed_column_names) const {
  std::string suffix;
  for (std::size_t index = 0; index < indexed_column_names.size(); ++index) {
    if (index > 0) {
      suffix += "__";
    }
    suffix += indexed_column_names[index];
  }
  return table_name + (suffix.empty() ? "" : "." + suffix) + ".index";
}

void LocalStorageManager::createHeap(const std::string& file_name) const {
  if (exists(file_name)) {
    throw std::runtime_error("Heap file already exists: " + file_name);
  }
  std::filesystem::create_directories(root_);
  HeapFile(pathFor(file_name)).initialize();
}

void LocalStorageManager::createIndex(const std::string& file_name) const {
  if (exists(file_name)) {
    throw std::runtime_error("Index file already exists: " + file_name);
  }
  std::filesystem::create_directories(root_);
  PagedFile index_file(pathFor(file_name));
  std::array<char, Page::PAGE_SIZE_BYTE> index_root_buffer{};
  Page(index_root_buffer.data(), PageKind::LeafIndex,
       LeafIndexPage::NO_RIGHT_SIBLING, 0);
  index_file.writePageFromBuffer(0, index_root_buffer.data());
}

void LocalStorageManager::remove(const std::string& file_name) const {
  const std::string path = pathFor(file_name);
  PagedFile::invalidateCache(path);
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove storage file: " + path +
                             ": " + error.message());
  }
}

bool LocalStorageManager::exists(const std::string& file_name) const {
  return std::filesystem::exists(pathFor(file_name));
}

HeapFile LocalStorageManager::openHeap(const std::string& file_name) const {
  if (!exists(file_name)) {
    throw std::runtime_error("Heap file does not exist: " + file_name);
  }
  return HeapFile(pathFor(file_name));
}

PagedFile LocalStorageManager::openIndex(const std::string& file_name) const {
  if (!exists(file_name)) {
    throw std::runtime_error("Index file does not exist: " + file_name);
  }
  return PagedFile(pathFor(file_name));
}

std::string LocalStorageManager::pathFor(const std::string& file_name) const {
  const std::filesystem::path relative_path(file_name);
  if (file_name.empty() || relative_path.is_absolute() ||
      relative_path.has_parent_path() ||
      relative_path.filename() != file_name) {
    throw std::invalid_argument("Invalid local storage file name: " +
                                file_name);
  }
  return (root_ / relative_path).string();
}
