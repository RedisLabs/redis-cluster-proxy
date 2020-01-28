/*
 * Copyright (C) 2020  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __REDIS_CLUSTER_PROXY_ASSERT_H__
#define __REDIS_CLUSTER_PROXY_ASSERT_H__

#include <unistd.h> /* for _exit() */

#define assert(_e) ((_e)?(void)0 : (_proxyAssert(#_e,__FILE__,__LINE__),_exit(1)))

void _proxyAssert(char *estr, char *file, int line);

#endif
