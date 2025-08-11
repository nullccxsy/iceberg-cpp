/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <arrow/filesystem/localfs.h>
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_reader.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/schema.h"
#include "matchers.h"
#include "temp_file_test_base.h"
#include "test_common.h"

namespace iceberg {

class ManifestListReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { avro::AvroReader::Register(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
  }

  std::vector<ManifestFile> PrepareTestManifestList() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix = "/tmp/db/db/iceberg_test/metadata/";
    std::vector<std::string> paths = {"2bccd69e-d642-4816-bba0-261cd9bd0d93-m0.avro",
                                      "9b6ffacd-ef10-4abf-a89c-01c733696796-m0.avro",
                                      "2541e6b5-4923-4bd5-886d-72c6f7228400-m0.avro",
                                      "3118c801-d2e0-4df6-8c7a-7d4eaade32f8-m0.avro"};
    std::vector<int64_t> file_size = {7433, 7431, 7433, 7431};
    std::vector<int64_t> snapshot_id = {7412193043800610213, 5485972788975780755,
                                        1679468743751242972, 1579605567338877265};
    std::vector<std::vector<uint8_t>> bounds = {{'x', ';', 0x07, 0x00},
                                                {'(', 0x19, 0x07, 0x00},
                                                {0xd0, 0xd4, 0x06, 0x00},
                                                {0xb8, 0xd4, 0x06, 0x00}};
    for (int i = 0; i < 4; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.content = ManifestFile::Content::kData;
      manifest_file.sequence_number = 4 - i;
      manifest_file.min_sequence_number = 4 - i;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 1;
      manifest_file.existing_files_count = 0;
      manifest_file.deleted_files_count = 0;
      manifest_file.added_rows_count = 1;
      manifest_file.existing_rows_count = 0;
      manifest_file.deleted_rows_count = 0;
      PartitionFieldSummary partition;
      partition.contains_null = false;
      partition.contains_nan = false;
      partition.lower_bound = bounds[i];
      partition.upper_bound = bounds[i];
      manifest_file.partitions.emplace_back(partition);
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  std::vector<ManifestFile> PrepareTestManifestListPartition() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix = "iceberg-warehouse/db/v1_partition_test/metadata/";
    std::vector<std::string> paths = {"eafd2972-f58e-4185-9237-6378f564787e-m1.avro",
                                      "eafd2972-f58e-4185-9237-6378f564787e-m0.avro"};
    std::vector<int64_t> file_size = {6185, 6113};
    std::vector<int64_t> snapshot_id = {7532614258660258098, 7532614258660258098};

    std::vector<std::vector<std::uint8_t>> lower_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x30, 0x32, 0x2D, 0x32, 0x32},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32}};

    std::vector<std::vector<std::uint8_t>> upper_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33}};

    for (int i = 0; i < 2; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 4 * (1 - i);
      manifest_file.existing_files_count = 0;
      manifest_file.deleted_files_count = 2 * i;
      manifest_file.added_rows_count = 6 * (1 - i);
      manifest_file.existing_rows_count = 0;
      manifest_file.deleted_rows_count = 6 * i;

      PartitionFieldSummary partition;
      partition.contains_null = false;
      partition.contains_nan = false;
      partition.lower_bound = lower_bounds[i];
      partition.upper_bound = upper_bounds[i];
      manifest_file.partitions.emplace_back(partition);
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  std::vector<ManifestFile> PrepareTestManifestListComplexType() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix = "iceberg-warehouse/db/v1_type_test/metadata/";
    std::vector<std::string> paths = {"aeffe099-3bac-4011-bc17-5875210d8dc0-m1.avro",
                                      "aeffe099-3bac-4011-bc17-5875210d8dc0-m0.avro"};
    std::vector<int64_t> file_size = {6498, 6513};
    std::vector<int64_t> snapshot_id = {4134160420377642835, 4134160420377642835};

    for (int i = 0; i < 2; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 1 - i;
      manifest_file.existing_files_count = 0;
      manifest_file.deleted_files_count = i;
      manifest_file.added_rows_count = 2 * (1 - i);
      manifest_file.existing_rows_count = 0;
      manifest_file.deleted_rows_count = 3 * i;
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  std::vector<ManifestFile> PrepareTestManifestListPartitionComplex() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix =
        "iceberg-warehouse/db2/v1_complex_partition_test/metadata/";
    std::vector<std::string> paths = {"5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m0.avro",
                                      "5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m1.avro"};
    std::vector<int64_t> file_size = {6402, 6318};
    std::vector<int64_t> snapshot_id = {7522296285847100621, 7522296285847100621};

    std::vector<std::vector<std::uint8_t>> lower_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
        {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
        {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

    std::vector<std::vector<std::uint8_t>> upper_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x34},
        {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33},
        {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

    for (int i = 0; i < 2; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 0;
      manifest_file.existing_files_count = i == 0 ? 3 : 1;
      manifest_file.deleted_files_count = 1;
      manifest_file.added_rows_count = 0;
      manifest_file.existing_rows_count = i == 0 ? 4 : 1;
      manifest_file.deleted_rows_count = i == 0 ? 2 : 1;

      PartitionFieldSummary partition;
      for (int j = 0; j < 2; ++j) {
        partition.contains_null = false;
        partition.contains_nan = false;
        partition.lower_bound = lower_bounds[2 * i + j];
        partition.upper_bound = upper_bounds[2 * i + j];
        manifest_file.partitions.emplace_back(partition);
      }
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_F(ManifestListReaderTest, BasicTest) {
  std::string path = GetResourcePath(
      "snap-7412193043800610213-1-2bccd69e-d642-4816-bba0-261cd9bd0d93.avro");
  auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
  ASSERT_EQ(manifest_reader_result.has_value(), true);
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Files();
  ASSERT_EQ(read_result.has_value(), true);
  ASSERT_EQ(read_result.value().size(), 4);

  auto expected_manifest_list = PrepareTestManifestList();
  ASSERT_EQ(read_result.value(), expected_manifest_list);
}

TEST_F(ManifestListReaderTest, PartitionTest) {
  std::string path = GetResourcePath(
      "snap-7532614258660258098-1-eafd2972-f58e-4185-9237-6378f564787e.avro");
  auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
  ASSERT_EQ(manifest_reader_result.has_value(), true);
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Files();
  ASSERT_EQ(read_result.has_value(), true);
  ASSERT_EQ(read_result.value().size(), 2);

  auto expected_manifest_list = PrepareTestManifestListPartition();
  ASSERT_EQ(read_result.value(), expected_manifest_list);
}

TEST_F(ManifestListReaderTest, ComplexTypeTest) {
  std::string path = GetResourcePath(
      "snap-4134160420377642835-1-aeffe099-3bac-4011-bc17-5875210d8dc0.avro");
  auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
  ASSERT_EQ(manifest_reader_result.has_value(), true);
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Files();
  ASSERT_EQ(read_result.has_value(), true);
  ASSERT_EQ(read_result.value().size(), 2);

  auto expected_manifest_list = PrepareTestManifestListComplexType();
  ASSERT_EQ(read_result.value(), expected_manifest_list);
}

TEST_F(ManifestListReaderTest, PartitionComplexTypeTest) {
  std::string path = GetResourcePath(
      "snap-7522296285847100621-1-5d690750-8fb4-4cd1-8ae7-85c7b39abe14.avro");
  auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
  ASSERT_EQ(manifest_reader_result.has_value(), true);
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Files();
  ASSERT_EQ(read_result.has_value(), true);
  ASSERT_EQ(read_result.value().size(), 2);

  auto expected_manifest_list = PrepareTestManifestListPartitionComplex();
  ASSERT_EQ(read_result.value(), expected_manifest_list);
}

}  // namespace iceberg
