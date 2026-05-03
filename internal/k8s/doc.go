/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package k8s provides the tiny slice of the Kubernetes API tack's Tekton
// provider needs.
//
// The package is based on the shape and behavior of Kubernetes client-go's
// in-cluster REST client and watch plumbing, but reimplemented here with a much
// smaller API surface so tack doesn't need to import the full client-go module
// graph just to create resources, watch them, and stream pod logs.
//
// The Apache 2.0 notice above records that upstream client-go provenance in one
// place for the package.
package k8s
