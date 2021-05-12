/*
Copyright 2019 The Kubernetes Authors.

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

package testsuites

import (
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/testsuites"

	"github.com/onsi/ginkgo"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	azDiskClientSet "sigs.k8s.io/azuredisk-csi-driver/pkg/apis/client/clientset/versioned"
)

// DynamicallyProvisionedCmdVolumeTest will provision required StorageClass(es), PVC(s) and Pod(s)
// Waiting for the PV provisioner to create a new PV
// Testing if the Pod(s) Cmd is run with a 0 exit code
type DynamicallyProvisionedCmdVolumeTest struct {
	CSIDriver              driver.DynamicPVTestDriver
	Pods                   []PodDetails
	StorageClassParameters map[string]string
}

func (t *DynamicallyProvisionedCmdVolumeTest) Run(client clientset.Interface, namespace *v1.Namespace, azDiskClient *azDiskClientSet.Clientset, schedulerName string, isV2Driver bool) {
	for _, pod := range t.Pods {
		tpod, cleanup := pod.SetupWithDynamicVolumes(client, namespace, t.CSIDriver, t.StorageClassParameters, schedulerName)
		// defer must be called here for resources not get removed before using them
		for i := range cleanup {
			defer cleanup[i]()
		}

		ginkgo.By("deploying the pod")
		tpod.Create()
		defer tpod.Cleanup()
		ginkgo.By("checking that the pod's command exits with no error")
		tpod.WaitForSuccess()

		err := testsuites.ValidateAzVolumeAttachment(namespace, tpod, client, azDiskClient, t.StorageClassParameters, isV2Driver)
		framework.ExpectNoError(err)
		err = testsuites.ValidateAzVolume(namespace, tpod, client, azDiskClient, t.StorageClassParameters, isV2Driver)
		framework.ExpectNoError(err)
	}
}
