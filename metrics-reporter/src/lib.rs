/*
 *
 *  * This file is part of OpenTSDB.
 *  * Copyright (C) 2021  Yahoo.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
/// Trait for reporting metrics to a monitoring system.
/// Plugins implementing this trait will be dynamically loaded during start of the binary.
/// An example NoopMetricReporter is provided.
pub trait MetricsReporter: Send + Sync {
    fn build(self) -> Self where Self: Sized;
    fn count(&self, metric: &str, tags: &[&str], value: u64);
    fn gauge(&self, metric: &str, tags: &[&str], value: u64);
}

