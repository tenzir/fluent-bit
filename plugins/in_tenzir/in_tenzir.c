/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
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

#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_engine.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_utils.h>

#include <msgpack.h>

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "in_tenzir.h"

static inline void consume_bytes(char *buf, int bytes, int length)
{
    memmove(buf, buf + bytes, length - bytes);
}

static inline int process_pack(struct flb_in_tenzir_config *ctx,
                               char *data, size_t data_size)
{
    struct flb_log_event_decoder log_decoder;
    struct flb_log_event         log_event;
    msgpack_unpacked             result;
    msgpack_object               entry;
    int                          ret;
    size_t                       off;

    ret = flb_log_event_decoder_init(&log_decoder, NULL, 0);

    if (ret != FLB_EVENT_ENCODER_SUCCESS) {
        return -1;
    }

    /* Queue the data with time field */
    msgpack_unpacked_init(&result);

    off = 0;
    while (msgpack_unpack_next(&result, data, data_size, &off) == MSGPACK_UNPACK_SUCCESS) {
        entry = result.data;

        if (entry.type == MSGPACK_OBJECT_MAP) {
            ret = flb_log_event_encoder_begin_record(ctx->log_encoder);

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_set_current_timestamp(ctx->log_encoder);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_set_body_from_msgpack_object(
                        ctx->log_encoder, &entry);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_commit_record(ctx->log_encoder);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = 0;
            }
            else {
                ret = -1;

                break;
            }
        }
        else if (entry.type == MSGPACK_OBJECT_ARRAY) {
            ret = flb_event_decoder_decode_object(&log_decoder,
                                                  &log_event,
                                                  &entry);

            if (ret != FLB_EVENT_ENCODER_SUCCESS) {
                ret = -1;

                break;
            }

            ret = flb_log_event_encoder_begin_record(ctx->log_encoder);

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_set_timestamp(ctx->log_encoder,
                                                          &log_event.timestamp);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_set_metadata_from_msgpack_object(
                        ctx->log_encoder, log_event.metadata);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_set_body_from_msgpack_object(
                        ctx->log_encoder, log_event.body);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = flb_log_event_encoder_commit_record(ctx->log_encoder);
            }

            if (ret == FLB_EVENT_ENCODER_SUCCESS) {
                ret = 0;
            }
            else {
                ret = -1;

                break;
            }
        }
        else {
            /*
             * Upon exception, acknowledge the user about the problem but continue
             * working, do not discard valid JSON entries.
             */
            flb_plg_error(ctx->ins, "invalid record found, "
                          "it's not a JSON map or array");
            ret = -1;
            break;
        }
    }

    flb_log_event_decoder_destroy(&log_decoder);

    msgpack_unpacked_destroy(&result);

    return ret;
}

/* When Fluent Bit calls us, we perform the following steps:
 *
 *     1. Lock the shared buffer.
 *     2. Parse the JSON and deliver the generated events (like in_stdin)
 *     3. Unlock the shared shared buffer.
 */
static int in_tenzir_collect(struct flb_input_instance *ins,
                             struct flb_config *config, void *in_context)
{
    int pack_size;
    int ret;
    char *pack;
    struct flb_in_tenzir_config *ctx = in_context;

    pthread_mutex_lock(&ctx->shared->lock);

    if (ctx->shared->len <= 0) {
        flb_plg_trace(ctx->ins, "no new data available, pausing collector");
        flb_input_collector_pause(ctx->collector, ctx->ins);
        flb_engine_exit(config);
        /* Arguably, this is not an error, but in_stdin.c returns -1 when
         * pausing the collector, so we do the same. */
        goto error;
    }

    /* Parse the JSON and deliver the generated events. */
    flb_plg_trace(ctx->ins, "processing %i bytes: %s", ctx->shared->len,
                  ctx->shared->buf);
    ret = flb_pack_json_state(ctx->shared->buf, ctx->shared->len,
                              &pack, &pack_size, &ctx->pack_state);
    // We're not checking for FLB_ERR_JSON_PART because our JSON parser
    // never generates partial JSON.
    if (ret == FLB_ERR_JSON_INVAL) {
        flb_plg_debug(ctx->ins, "invalid JSON message, skipping");
        flb_pack_state_reset(&ctx->pack_state);
        flb_pack_state_init(&ctx->pack_state);
        ctx->pack_state.multiple = FLB_TRUE;
        ctx->shared->len = 0;
        goto error;
    }

    /* Process valid packaged records */
    process_pack(ctx, pack, pack_size);

    /* Move out processed bytes */
    consume_bytes(ctx->shared->buf, ctx->pack_state.last_byte,
                  ctx->shared->len);
    ctx->shared->len -= ctx->pack_state.last_byte;
    ctx->shared->buf[ctx->shared->len] = '\0';

    flb_pack_state_reset(&ctx->pack_state);
    flb_pack_state_init(&ctx->pack_state);
    ctx->pack_state.multiple = FLB_TRUE;

    flb_free(pack);

    if (ctx->log_encoder->output_length > 0) {
        flb_input_log_append(ctx->ins, NULL, 0,
                             ctx->log_encoder->output_buffer,
                             ctx->log_encoder->output_length);
    }

    flb_log_event_encoder_reset(ctx->log_encoder);

    pthread_mutex_unlock(&ctx->shared->lock);
    return 0;

error:
    pthread_mutex_unlock(&ctx->shared->lock);
    return 1;
}

/* Read tenzir config*/
static int in_tenzir_config_init(struct flb_in_tenzir_config *ctx,
                                 struct flb_input_instance *in,
                                 struct flb_config *config)
{
    int ret;

    ctx->ins = in;

    ret = flb_input_config_map_set(in, (void *)ctx);
    if (ret == -1) {
        return -1;
    }

    return 0;
}

static void in_tenzir_config_destroy(struct flb_in_tenzir_config *ctx)
{
    if (!ctx) {
        return;
    }

    if (ctx->log_encoder != NULL) {
        flb_log_event_encoder_destroy(ctx->log_encoder);
    }

    flb_free(ctx);
}

/* Initialize plugin */
static int in_tenzir_init(struct flb_input_instance *in,
                          struct flb_config *config, void *data)
{
    int ret;
    struct flb_in_tenzir_config *ctx;

    /* Allocate space for the configuration context */
    ctx = flb_calloc(1, sizeof(struct flb_in_tenzir_config));
    if (!ctx) {
        return -1;
    }

    /* Accept shared state. */
    if (data == NULL) {
        flb_plg_error(in, "could not find shared Tenzir state");
        goto init_error;
    }
    ctx->shared = (struct shared_state*)data;

    ctx->ins = in;

    ctx->log_encoder =
        flb_log_event_encoder_create(FLB_LOG_EVENT_FORMAT_DEFAULT);
    if (ctx->log_encoder == NULL) {
        flb_plg_error(in, "could not initialize event encoder");
        goto init_error;
    }

    /* Initialize stdin config */
    ret = in_tenzir_config_init(ctx, in, config);
    if (ret < 0) {
        goto init_error;
    }

    /* Always initialize built-in JSON pack state */
    flb_pack_state_init(&ctx->pack_state);
    ctx->pack_state.multiple = FLB_TRUE;

    /* Set the context */
    flb_input_set_context(in, ctx);

    /* Trigger collection callback in a time-based manner */
    time_t seconds = 0;
    long nanoseconds = 1 * 1000 * 1000;
    ret = flb_input_set_collector_time(in, in_tenzir_collect, seconds,
                                       nanoseconds, config);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "Could not set collector for Tenzir input plugin");
        goto init_error;
    }
    ctx->collector = ret;

    return 0;

init_error:
    in_tenzir_config_destroy(ctx);
    return -1;
}

/* Cleanup serial input */
static int in_tenzir_exit(void *in_context, struct flb_config *config)
{
    struct flb_in_tenzir_config *ctx = in_context;

    if (!ctx) {
        return 0;
    }

    flb_pack_state_reset(&ctx->pack_state);
    in_tenzir_config_destroy(ctx);

    return 0;
}

static struct flb_config_map config_map[] = {
    /* EOF */
    {0}
};

/* Plugin reference */
struct flb_input_plugin in_tenzir_plugin = {
    .name         = "tenzir",
    .description  = "Tenzir",
    .cb_init      = in_tenzir_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_tenzir_collect,
    .cb_flush_buf = NULL,
    .cb_exit      = in_tenzir_exit,
    .config_map   = config_map
};
