/*
 * acp-c : Arcus C Client Performance benchmark program
 * Copyright 2013-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <netinet/in.h>
#include <assert.h>
#include <time.h>

#include "libmemcached/memcached.h"
#include "common.h"
#include "keyset.h"
#include "valueset.h"
#include "client_profile.h"
#include "client.h"

static int print_check(int ok, memcached_return rc)
{
  if (ok) return 0;
  if (rc == MEMCACHED_ELEMENT_EXISTS || rc == MEMCACHED_EXISTS
      || rc == MEMCACHED_TYPE_MISMATCH) {
    return 0;
  }
  return 1;
}

static int
do_btree_test(struct client *cli)
{
  memcached_coll_create_attrs_st attr;
  memcached_return rc;
  int i, ok, keylen;
  uint64_t bkey;

  // Pick a key
  const char *key = keyset_get_key_by_cliid(cli->ks, cli);
  keylen = strlen(key);

  // Create a btree item
  if (0 != client_before_request(cli))
    return -1;

  memcached_coll_create_attrs_init(&attr, 20 /* flags */, 60 /* exptime */,
    4000 /* maxcount */);
  memcached_coll_create_attrs_set_overflowaction(&attr,
    OVERFLOWACTION_SMALLEST_TRIM);
  rc = memcached_bop_create(cli->next_mc, key, keylen, &attr);
  ok = (rc == MEMCACHED_SUCCESS);
  if (print_check(ok, rc)) {
    print_log("bop create failed. id=%d key=%s rc=%d(%s)", cli->id, key,
              rc, memcached_detail_error_message(cli->next_mc, rc));
  }
  if (0 != client_after_request(cli, ok))
    return -1;

  if (ok) {
  // Insert a number of btree element
  for (i = 0; i < 10; i++) {
    uint8_t *val_ptr;
    int val_len;

    if (0 != client_before_request(cli))
      return -1;

    bkey = 1234 + i;

    val_ptr = NULL;
    val_len = valueset_get_value(cli->vs, &val_ptr);
    assert(val_ptr != NULL && val_len > 0 && val_len <= 4096);

    rc = memcached_bop_insert(cli->next_mc, key, keylen,
      bkey,
      NULL /* eflag */, 0 /* eflag length */,
      (const char*)val_ptr, (size_t)val_len,
      NULL /* Do not create automatically */);
    valueset_return_value(cli->vs, val_ptr);
    ok = (rc == MEMCACHED_SUCCESS);
    if (print_check(ok, rc)) {
      print_log("bop insert failed. id=%d key=%s bkey=%llu rc=%d(%s)",
        cli->id, key, (long long unsigned)bkey,
        rc, memcached_detail_error_message(cli->next_mc, rc));
    }
    if (0 != client_after_request(cli, ok))
      return -1;
  }
  }

  // Update
  // Get/delete
  // Upsert
  // Incr/decr
  // Expire time
  // Smget
  // Getattr
  // PipedInsert
  return 0;
}

static int
do_set_test(struct client *cli)
{
  memcached_return rc;
  memcached_coll_create_attrs_st attr;
  int i, ok, keylen;
  const char *key;

  // Pick a key
  key = keyset_get_key_by_cliid(cli->ks, cli);
  keylen = strlen(key);

  // Create a set item
  if (0 != client_before_request(cli))
    return -1;

  memcached_coll_create_attrs_init(&attr, 10 /* flags */, 60 /* exptime */,
    4000 /* maxcount */);
  memcached_coll_create_attrs_set_overflowaction(&attr, OVERFLOWACTION_ERROR);
  rc = memcached_sop_create(cli->next_mc, key, keylen, &attr);
  ok = (rc == MEMCACHED_SUCCESS);
  if (print_check(ok, rc)) {
    print_log("sop create failed. id=%d key=%s rc=%d(%s)", cli->id, key,
      rc, memcached_detail_error_message(cli->next_mc, rc));
  }
  if (0 != client_after_request(cli, ok))
    return -1;

  if (ok) {
  // Insert a number of elements.  Set has no element keys.
  for (i = 0; i < 10; i++) {
    uint8_t *val_ptr;
    int val_len;

    if (0 != client_before_request(cli))
      return -1;

    val_ptr = NULL;
    val_len = valueset_get_value(cli->vs, &val_ptr);
    assert(val_ptr != NULL && val_len > 0 && val_len <= 4096);

    rc = memcached_sop_insert(cli->next_mc, key, keylen,
      (const char*)val_ptr, (size_t)val_len,
      NULL /* Do not create automatically */);
    valueset_return_value(cli->vs, val_ptr);
    ok = (rc == MEMCACHED_SUCCESS);
    if (print_check(ok, rc)) {
      print_log("sop insert failed. id=%d key=%s val_len=%d rc=%d(%s)",
        cli->id, key, val_len, rc, memcached_detail_error_message(cli->next_mc, rc));
    }
    if (0 != client_after_request(cli, ok))
      return -1;
  }
  }

  // Get/delete
  // Delete
  // PipedInsertBulk
  // Expire time
  // Getattr
  return 0;
}

static int
do_list_test(struct client *cli)
{
  memcached_return rc;
  memcached_coll_create_attrs_st attr;
  int i, ok, keylen;
  const char *key;

  // Pick a key
  key = keyset_get_key_by_cliid(cli->ks, cli);
  keylen = strlen(key);

  // Create a list item
  if (0 != client_before_request(cli))
    return -1;

  memcached_coll_create_attrs_init(&attr, 10 /* flags */, 60 /* exptime */,
    4000 /* maxcount */);
  memcached_coll_create_attrs_set_overflowaction(&attr,
    OVERFLOWACTION_TAIL_TRIM);
  rc = memcached_lop_create(cli->next_mc, key, keylen, &attr);
  ok = (rc == MEMCACHED_SUCCESS);
  if (print_check(ok, rc)) {
    print_log("lop create failed. id=%d key=%s rc=%d(%s)", cli->id, key,
      rc, memcached_detail_error_message(cli->next_mc, rc));
  }
  if (0 != client_after_request(cli, ok))
    return -1;

  // Insert a number of elements.  Push at the head.
  if (ok) {
  for (i = 0; i < 10; i++) {
    uint8_t *val_ptr;
    int val_len;

    if (0 != client_before_request(cli))
      return -1;

    val_ptr = NULL;
    val_len = valueset_get_value(cli->vs, &val_ptr);
    assert(val_ptr != NULL && val_len > 0 && val_len <= 4096);

    rc = memcached_lop_insert(cli->next_mc, key, keylen,
      0 /* 0=head */,
      (const char*)val_ptr, (size_t)val_len,
      NULL /* Do not create automatically */);
    valueset_return_value(cli->vs, val_ptr);
    ok = (rc == MEMCACHED_SUCCESS);
    if (print_check(ok, rc)) {
      print_log("lop insert failed. id=%d key=%s rc=%d(%s)", cli->id, key,
        rc, memcached_detail_error_message(cli->next_mc, rc));
    }
    if (0 != client_after_request(cli, ok))
      return -1;
  }
  }

  // Get/delete
  // Delete
  // PipedInsert
  // Expire time
  // Getattr
  return 0;
}

 static int
 do_map_test(struct client *cli)
 {
     memcached_return rc;
     memcached_coll_create_attrs_st attr;
     int i, ok, keylen;
     const char *key;

     // Pick a key
     key = keyset_get_key_by_cliid(cli->ks, cli);
     keylen = strlen(key);

     // Create a list item
     if (0 != client_before_request(cli))
     return -1;

     memcached_coll_create_attrs_init(&attr, 10 /* flags */, 60 /* exptime */,
     4000 /* maxcount */);
     memcached_coll_create_attrs_set_overflowaction(&attr,
     OVERFLOWACTION_ERROR);
     rc = memcached_mop_create(cli->next_mc, key, keylen, &attr);
     ok = (rc == MEMCACHED_SUCCESS);
     if (print_check(ok, rc)) {
     print_log("mop create failed. id=%d key=%s rc=%d(%s)", cli->id, key,
     rc, memcached_detail_error_message(cli->next_mc, rc));
     }
     if (0 != client_after_request(cli, ok))
     return -1;

     if (ok) {
     char *mkey = (char*)malloc(10);
     int len = 0;
     // Insert a number of elements.
     for (i = 0; i < 10; i++) {
     uint8_t *val_ptr;
     int val_len;

     if (0 != client_before_request(cli))
     return -1;

     len = sprintf(mkey, "%d", i);

     val_ptr = NULL;
     val_len = valueset_get_value(cli->vs, &val_ptr);
     assert(val_ptr != NULL && val_len > 0 && val_len <= 4096);

      rc = memcached_mop_insert(cli->next_mc, key, keylen,
        (const char*)mkey, len, (const char*)val_ptr, (size_t)val_len,
        NULL /* Do not create automatically */);
      valueset_return_value(cli->vs, val_ptr);
      ok = (rc == MEMCACHED_SUCCESS);
      if (print_check(ok, rc)) {
        print_log("mop insert failed. id=%d key=%s mkey=%.*s rc=%d(%s)",
          cli->id, key, len, mkey,
          rc, memcached_detail_error_message(cli->next_mc, rc));
      }
      if (0 != client_after_request(cli, ok))
        return -1;
     }
     free(mkey);
     }
     // Get/delete
     // Delete
     // PipedInsert
     // Expire time
     // Getattr
     return 0;
 }

static int
do_simple_test(struct client *cli)
{
  memcached_return rc;
  int i, ok, keylen;
  const char *key;
  uint8_t *val_ptr;
  int val_len;

  // Set a number of items
  for (i = 0; i < 1; i++) {
    if (0 != client_before_request(cli))
      return -1;

    // Pick a key
    key = keyset_get_key_by_cliid(cli->ks, cli);
    keylen = strlen(key);

    // Pick a value
    val_ptr = NULL;
    val_len = valueset_get_value(cli->vs, &val_ptr);
    assert(val_ptr != NULL && val_len > 0);

    rc = memcached_set(cli->next_mc, key, keylen, (const char*)val_ptr,
      (size_t)val_len, 60 /* exptime */, 0 /* flags */);
    valueset_return_value(cli->vs, val_ptr);
    ok = (rc == MEMCACHED_SUCCESS);
    if (print_check(ok, rc)) {
      print_log("set failed. id=%d key=%s rc=%d(%s)", cli->id, key,
        rc, memcached_detail_error_message(cli->next_mc, rc));
    }
    if (0 != client_after_request(cli, ok))
      return -1;
  }

  // Incr/decr
  // Get/delete
  // Update
  // Multiget
  // Expire time
  // Getattr
  // Cas
  // Add
  // Replace
  return 0;
}

#define MSET_COUNT 10
#define BUFFER_SIZE 64
#define EXPIRE_TIME 3600

memcached_return_t memcached_mset(memcached_st *ptr,
                                  const memcached_storage_request_st *req,
                                  const size_t number_of_req,
                                  memcached_return_t *results) __attribute__((weak));

static inline void safe_free(void *ptr)
{
  if (ptr != NULL)
  {
    free(ptr);
  }
}

static inline void safe_free_req(memcached_storage_request_st req[MSET_COUNT])
{
  if (req == NULL)
  {
    return;
  }

  int i;
  for (i= 0; i < MSET_COUNT; i++)
  {
    safe_free(req[i].key);
    safe_free(req[i].value);
  }
}

static int
do_mset_test(struct client *cli)
{
  if (!memcached_mset)
  {
    return 0;
  }

  memcached_storage_request_st req[MSET_COUNT];
  memcached_return_t results[MSET_COUNT];
  size_t i;

  for (i = 0; i < MSET_COUNT; i++)
  {
    const char *key_base = keyset_get_key_by_cliid(cli->ks, cli);

    char *key = (char *)malloc(BUFFER_SIZE);
    if (key == NULL)
    {
      print_log("mset failed while allocating key. id=%d", cli->id);
      return -1;
    }

    char *value = (char *)malloc(BUFFER_SIZE);
    if (value == NULL)
    {
      print_log("mset failed while allocating value. id=%d key=%s", cli->id, key);
      safe_free(key);
      return -1;
    }

    memset(key, 0, BUFFER_SIZE);
    snprintf(key, BUFFER_SIZE, "%s-%lu", key_base, i);

    memset(value, 0, BUFFER_SIZE);
    snprintf(value, BUFFER_SIZE, "mset-value-%lu-%u", i, (uint32_t) rand());

    req[i].key = key;
    req[i].key_length = strlen(key);

    req[i].value = value;
    req[i].value_length = strlen(value);

    req[i].expiration = EXPIRE_TIME;
    req[i].flags = (uint32_t) rand();
  }

  if (0 != client_before_request(cli))
  {
    return -1;
  }

  memcached_st *mc= cli->next_mc;
  memcached_return_t rc = memcached_mset(mc, req, MSET_COUNT, results);
  if (memcached_failed(rc))
  {
    print_log("mset failed. id=%d rc=%d(%s)",
              cli->id, rc, memcached_detail_error_message(mc, rc));
    return client_after_request(cli, false);
  }

  for (i = 0; i < MSET_COUNT; i++)
  {
    if (memcached_failed(results[i]))
    {
      print_log("mset failed. id=%d rc[%lu]=%d(%s)",
                cli->id, i, rc, memcached_detail_error_message(mc, rc));
      safe_free_req(req);
      return client_after_request(cli, false);
    }

    size_t value_length = -1;
    uint32_t flags = -1;
    char *value = memcached_get(mc, req[i].key, req[i].key_length, &value_length, &flags, &rc);

    if (value == NULL || memcached_failed(rc))
    {
      print_log("get after mset failed. id=%d key=%s rc[%lu]=%d(%s)",
                cli->id, req[i].key, i, rc, memcached_detail_error_message(mc, rc));
      safe_free(value);
      safe_free_req(req);
      return client_after_request(cli, false);
    }

    if (req[i].value_length != value_length)
    {
      print_log("get after mset failed for value_length. stored %ld but got %ld. id=%d key=%s rc[%lu]=%d(%s)",
                req[i].value_length, value_length,  cli->id, req[i].key, i, rc, memcached_detail_error_message(mc, rc));
      safe_free(value);
      safe_free_req(req);
      return client_after_request(cli, false);
    }
    if (strcmp(req[i].value, value))
    {
      print_log("get after mset failed for value. stored %s but got %s. id=%d key=%s rc[%lu]=%d(%s)",
                req[i].value, value, cli->id, req[i].key, i, rc, memcached_detail_error_message(mc, rc));
      safe_free(value);
      safe_free_req(req);
      return client_after_request(cli, false);
    }
    if (req[i].flags != flags)
    {
      print_log("get after mset failed for flags. stored %u but got %u. id=%d key=%s rc[%lu]=%d(%s)",
                req[i].flags, flags, cli->id, req[i].key, i, rc, memcached_detail_error_message(mc, rc));
      safe_free(value);
      safe_free_req(req);
      return client_after_request(cli, false);
    }

    safe_free(value);
  }

  safe_free_req(req);
  return client_after_request(cli, true);
}

static int
do_test(struct client *cli)
{
  if (0 != do_btree_test(cli))
    return -1;

  if (0 != do_map_test(cli))
    return -1;

  if (0 != do_set_test(cli))
    return -1;

  if (0 != do_list_test(cli))
    return -1;

  if (0 != do_simple_test(cli))
    return -1; // Stop the test

  if (0 != do_mset_test(cli))
    return -1;

  return 0; // Do another test
}

static struct client_profile default_profile = {
  .do_test = do_test,
};

struct client_profile *
standard_mix_init(void)
{
  srand(time(NULL));
  return &default_profile;
}
