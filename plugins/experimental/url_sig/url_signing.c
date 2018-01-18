
/** @file

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <getopt.h>

#include <arpa/inet.h>

#include <unistd.h>
#include <time.h>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <ctype.h>

#define MAX(a,b)   ( a > b ? a : b )
#define MIN(a,b)   ( a > b ? b : a )

int g_verbose_opt = 0;
int g_pathparams_opt = 0;

const char *g_url = NULL; // required
const char *g_key_str = NULL; // required

#define DIGEST_LEN_UINTS    SHA_DIGEST_LENGTH/sizeof(uint32_t) 


const char *g_useparts = "";
const char *g_client = NULL;
const char *g_proxy = NULL;
const char *g_siganchor = NULL;

const char *g_algorithm_str = NULL;
const char *g_keyindex_str = NULL;
const char *g_duration_str = NULL;

int g_algorithm = 1; // dflt
int g_keyindex = -1; // required
time_t g_expired = ~0LL;

int parse_options(int argc, char *argv[])
{
  int opt = -1;
  static struct option const s_opts[] = {
    // required args..
    {"url", 1, NULL, 0},
    {"key", 1, NULL, 0},
    {"algorithm", 1, NULL, 0},
    {"keyindex", 1, NULL, 0},
    // optional args..
    {"useparts", 1, NULL, 0},
    {"client", 1, NULL, 0},
    {"proxy", 1, NULL, 0},
    {"siganchor", 1, NULL, 0},
    {"duration", 1, NULL, 0},
    // simple flags
    {"verbose", 0, &g_verbose_opt, 1},
    {"pathparams", 0, &g_pathparams_opt, 1},
    { NULL, 0, NULL, 0}
  };

  static const char **const s_opt_args[] = {
    &g_url, &g_key_str, &g_algorithm_str, &g_keyindex_str,
    &g_useparts, &g_client, &g_proxy, &g_siganchor, &g_duration_str
  };

  while ( getopt_long_only(argc,argv,"",s_opts,&opt) == 0 ) {
    if ( optarg ) {
      (*s_opt_args[opt]) = strdup(optarg); // bool opts don't set args
    }
  }

  if ( ! g_key_str || ! g_url || ! g_duration_str || ! g_keyindex_str ) {
    return -1;
  }

  if ( g_proxy && strncmp("http://",g_proxy,7) ) {
    return -2;
  }

  if ( g_proxy && ( ! isdigit(g_proxy[7]) || ! isdigit(g_proxy[8]) ) ) {
    return -3;
  }

  g_keyindex = atoi(g_keyindex_str);
  g_expired = atoi(g_duration_str);

  assert(g_expired >= 0);

  g_expired += time(NULL);

  g_algorithm = ( g_algorithm_str ? atoi(g_algorithm_str) : 1 );

  return 0;
}

void help()
{
}

char *fmt_param_string(char sep)
{
  char buff[1024]; // must be big enough
  char *buffn = &buff[0];
  char *buffe = buffn + sizeof(buff);

  if ( g_client ) {
    buffn += snprintf(buffn,MAX(buffe-buffn,0),"%cC=%s",sep,g_client);
  }
  buffn += snprintf(buffn,MAX(buffe-buffn,0),
                    "%cE=%ld" "%cA=%d" "%cK=%d" "%cP=%s" "%cS=",
                     sep,g_expired,  
                               sep,g_algorithm,
                                       sep,g_keyindex,
                                                sep,g_useparts,
                                                         sep);

  assert( buffn < buffe );

  return strndup(buff,sizeof(buff));
}

void copy_allowed_path(char *encstring, const char *url, const char *tailend)
{
  encstring[0] = '\0';  // start with zero len

  // empty-default is *no* parts of URL
  const char *bit = ( g_useparts && *g_useparts ? g_useparts : "0" );

  for( const char *slashp = NULL
       ; (slashp=strchr(url+1,'/'))
         ; url=slashp )
  {
    if ( ! *encstring && *url == '/' ) {
      ++url; // skip a leading slash..
    }
    if ( *bit++ != '0' ) {
      strncat(encstring,url,slashp-url);
    }
    ( *bit ? bit : --bit ); // re-use last bit
  }

  if ( g_pathparams_opt ) {
    return; // must ignore last field of path...
  }

  // what's the last segment to copy?
  const char *filestr = ( *bit != '0' ? url : tailend );

  if ( filestr ) {
    strcat(encstring,filestr);
  }
}

void create_hex_digest(const char *orig, char *digest)
{
  unsigned bindigestlen = 0;
  uint32_t bindigest[DIGEST_LEN_UINTS] = { 0 };
  const EVP_MD *algo = ( g_algorithm == 1 ? EVP_sha1() : EVP_md5() );

  HMAC_CTX ctx;
  HMAC_CTX_init(&ctx);

  HMAC_Init(&ctx,g_key_str,strlen(g_key_str),algo);
  HMAC_Update(&ctx,(uint8_t*)orig,strlen(orig));
  HMAC_Final(&ctx, (uint8_t*)&bindigest,&bindigestlen); 
  if ( bindigestlen > sizeof(uint32_t)*4 ) {
    sprintf(digest,"%08x%08x%08x%08x%08x",
                ntohl(bindigest[0]), 
                ntohl(bindigest[1]), 
                ntohl(bindigest[2]), 
                ntohl(bindigest[3]),
                ntohl(bindigest[4]));
  } else {
    sprintf(digest,"%08x%08x%08x%08x",
                ntohl(bindigest[0]), 
                ntohl(bindigest[1]), 
                ntohl(bindigest[2]), 
                ntohl(bindigest[3]));
  }
}


int main(int argc, char *argv[])
{
  int r = parse_options(argc,argv);
  if ( r < 0 ) {
    help();
    exit(r);
  }

  const char *url = g_url;

  if ( ! strncmp("https://",g_url,8) ) {
    url += 8;
  } else if ( ! strncmp("http://",g_url,7) ) {
    url += 7;
  }

  char *lastslash = strrchr(g_url,'/');

  if ( g_pathparams_opt && ! lastslash ) {
    fprintf(stderr,"\nERROR: No file segment in the path when using --pathparams.\n\n");
    help();
    exit(-1);
  }

  char *pathparams = NULL;
  char *queryadd = NULL;
  char *querybase = ( lastslash ? strchr(lastslash,'?') : NULL );
  char *encstring = NULL;

  if ( g_pathparams_opt ) {
    pathparams = fmt_param_string(';');
    encstring = (char*) alloca( strlen(url) + strlen(pathparams)*2 );
  } else {
    queryadd = fmt_param_string('&');
    queryadd[0] = ( ! querybase ? '?' : '&' );
    encstring = (char*) alloca( strlen(url) + strlen(queryadd) );
  }

  copy_allowed_path(encstring,url,querybase);

  if ( queryadd ) {
    strcat(encstring,queryadd);
  } else if ( pathparams ) {
    strcat(encstring,pathparams);
  }

  int digestlen = DIGEST_LEN_UINTS*sizeof(uint32_t) * 2 + 1;
  char *digest = alloca(digestlen);

  create_hex_digest(encstring,digest);

  if ( g_verbose_opt ) {
    printf("Signed String: %s\n\n\n",encstring);
    printf("Url: %s\n\n",url);

    if ( g_pathparams_opt ) {
      printf("signing_signature: %s\n\n",pathparams);
    } else {
      printf("signing_signature: %s%s\n\n",( querybase ? : "" ),queryadd);
    }

    printf("digest: %s\n",digest);
  }

  if ( queryadd ) {
    strcat(queryadd,digest);
  } 
  if ( pathparams ) {
    strcat(pathparams,digest);
  }

  char *proxy = ( g_proxy ? alloca(strlen(g_proxy)+10) : "" );
  if ( g_proxy ) {
    snprintf(proxy, strlen(g_proxy)+10, "--proxy %s ",g_proxy);
  }

  char *encoded = NULL;
  BIO *mem = BIO_new(BIO_s_mem());
  BIO *tobase64 = BIO_new(BIO_f_base64());
  BIO_set_flags(tobase64,BIO_FLAGS_BASE64_NO_NL);

  if ( g_pathparams_opt ) {
    BIO_push(tobase64,mem);
    BIO_write(tobase64, pathparams, strlen(pathparams));
    BIO_flush(tobase64);
    BIO_get_mem_data(mem,&encoded);

    char *p;

    if ( (p=strchr(encoded,'=')) ) {
      *p = '\0';
    }

    for( p = encoded ; (p=strchr(encoded,'+')) ; ++p ) {
      *p = '-';
    }
    for( p = encoded ; (p=strchr(encoded,'/')) ; ++p ) {
      *p = '_';
    }
  }

  if ( ! g_pathparams_opt ) {
    printf("curl -s -o /dev/null -v -L --max-redirs 1 %s'%s%s'\n\n", proxy, g_url, queryadd);
  } else if ( g_siganchor ) {
    *lastslash = '\0'; // truncate
    printf("curl -s -o /dev/null -v -L --max-redirs 1 %s'%s;%s=%s/%s'\n\n", proxy, g_url, g_siganchor, encoded, lastslash+1);
  } else {
    *lastslash = '\0'; // truncate
    printf("curl -s -o /dev/null -v -L --max-redirs 1 %s'%s/%s/%s'\n\n", proxy, g_url, encoded, lastslash+1);
  }

  // create final request URL

  BIO_free_all(mem);

  return 0;
}
