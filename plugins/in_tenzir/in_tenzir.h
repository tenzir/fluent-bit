/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2023 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef FLB_IN_TENZIR_H
#define FLB_IN_TENZIR_H

#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_log_event_encoder.h>

#include <pthread.h>

/* Shared state between Tenzir and Fluent Bit.
 * WARNING: keep in sync with the respective code bases.
 */
struct shared_state {
    char            *buf;
    int             len;
    pthread_mutex_t lock;
};

/* Tenzir Input configuration & context */
struct flb_in_tenzir_config {
    /* Shared state with Tenzir */
    struct shared_state *shared;

    /* Fluent Bit state */
    int collector;
    struct flb_pack_state pack_state;
    struct flb_input_instance *ins;
    struct flb_log_event_encoder *log_encoder;
};

extern struct flb_input_plugin in_tenzir_plugin;

#endif
