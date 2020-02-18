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
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "client.h"
#include "config.h"

struct keyset {
  const char **set;
  int set_size;
  int next_idx;
  int offset;
  pthread_mutex_t lock;
};

void
keyset_reset(struct keyset *ks)
{
  ks->next_idx = 0;
}

struct keyset *
keyset_init(int num, const char *prefix)
{
  struct keyset *ks;
  char buf[128];
  int i;

  ks = malloc(sizeof(*ks));
  memset(ks, 0, sizeof(*ks));
  pthread_mutex_init(&ks->lock, NULL);
  ks->set_size = num;
  ks->set = malloc(num * sizeof(char*));
  ks->offset = 0;
  for (i = 0; i < num; i++) {
    if (prefix != NULL)
      sprintf(buf, "%stestkey한글-%d", prefix, i);
    else
      sprintf(buf, "testkey한글-%d", i);
    ks->set[i] = strdup(buf);
  }
  
  keyset_reset(ks);
  return ks;
}


const char *
keyset_get_key(struct keyset *ks, int *id)
{
  int idx;
  const char *key;
  
  pthread_mutex_lock(&ks->lock);
  idx = ks->next_idx;
  ks->next_idx++;
  if (ks->next_idx >= ks->set_size)
    ks->next_idx = 0;
  key = ks->set[idx];
  pthread_mutex_unlock(&ks->lock);
  if (id != NULL)
    *id = idx;
  return key;
}

const char *
keyset_get_key_by_cliid(struct keyset *ks, struct client *cli)
{
  const char *key;
  
  pthread_mutex_lock(&ks->lock);
  if (ks->offset == 0) ks->offset = ks->set_size / cli->conf->client;
  if (cli->keyidx >= ks->offset) cli->keyidx = 0;
  key = ks->set[cli->id * ks->offset + cli->keyidx++];
  pthread_mutex_unlock(&ks->lock);
  return key;
}
