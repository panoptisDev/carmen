// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisDev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "common/account_state.h"

#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;

TEST(AccountStateTest, ValueSize) { EXPECT_EQ(1, sizeof(AccountState)); }

TEST(AccountStateTest, IsTrivial) { EXPECT_TRUE(Trivial<AccountState>); }

TEST(AccountStateTest, IsPrintable) {
  EXPECT_THAT(Print(AccountState::kUnknown), StrEq("unknown"));
  EXPECT_THAT(Print(AccountState::kExists), StrEq("exists"));
  EXPECT_THAT(Print(AccountState(10)), StrEq("invalid"));
}

}  // namespace
}  // namespace carmen
