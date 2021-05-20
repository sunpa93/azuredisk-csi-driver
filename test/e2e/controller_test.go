/*
Copyright 2021 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/onsi/ginkgo"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	azDiskClientSet "sigs.k8s.io/azuredisk-csi-driver/pkg/apis/client/clientset/versioned"
)

const (
	partitionKey = "azdrivernodes.disk.csi.azure.com/partition"
)

var _ = ginkgo.Describe("Controller", func() {
	ginkgo.Context("[single-az]", func() {
		defineTests(false)
	})

	//TODO add support for scheduler extender
	ginkgo.Context("[multi-az]", func() {
		// skip multi zone test for now
		return
		// TODO
		//defineTests(true)
	})
})

// TODO: migrate to integration test suite
var defineTests = func(isMultiZone bool) {
	f := framework.NewDefaultFramework("azuredisk")

	var (
		cs clientset.Interface
		// ns           *v1.Namespace
		azDiskClient *azDiskClientSet.Clientset
		// testDriver   driver.PVTestDriver
		err error
	)

	// volumeNameSeed := "test-volume-"

	// var setUpPod = func(size, maxShares int) (*testsuites.TestPod, []func()) {
	// 	pvcSize := strconv.Itoa(size)
	// 	t := dynamicProvisioningTestSuite{}
	// 	pod := testsuites.PodDetails{
	// 		Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
	// 		Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
	// 			{
	// 				ClaimSize: pvcSize + "Gi",
	// 				MountOptions: []string{
	// 					"barrier=1",
	// 					"acl",
	// 				},
	// 				VolumeMount: testsuites.VolumeMountDetails{
	// 					NameGenerate:      volumeNameSeed,
	// 					MountPathGenerate: "/mnt/test-",
	// 				},
	// 			},
	// 		}, isMultiZone),
	// 		IsWindows: isWindowsCluster,
	// 	}
	// 	storageClassParameters := map[string]string{"skuName": "Premium_LRS", "maxShares": strconv.Itoa(maxShares)}
	// 	tpod, cleanups := pod.SetupWithDynamicVolumes(cs, ns, testDriver, storageClassParameters, schedulerName)
	// 	tpod.Create()
	// 	tpod.WaitForSuccess()

	// 	return tpod, cleanups
	// }

	// testDriver = driver.InitAzureDiskDriver()

	ginkgo.BeforeEach(func() {
		cs = f.ClientSet
		// ns = f.Namespace

		azDiskClient, err = azDiskClientSet.NewForConfig(f.ClientConfig())
		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Failed to create disk client. Error: %v", err))
		}
	})

	ginkgo.Context("AzDriverNode [single-az]", func() {
		ginkgo.It("Should create AzDriverNode resource and report heartbeat.", func() {
			skipIfUsingInTreeVolumePlugin()
			skipIfNotUsingCSIDriverV2()

			pods, err := cs.CoreV1().Pods("kube-system").List(context.TODO(), metav1.ListOptions{})
			framework.ExpectNoError(err)

			for _, pod := range pods.Items {
				if strings.Contains(pod.Spec.NodeName, "csi-azuredisk-node") {
					azN := azDiskClient.DiskV1alpha1().AzDriverNodes("azure-disk-csi")
					dNode, err := azN.Get(context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
					framework.ExpectNoError(err)
					ginkgo.By("Checking AzDriverNode/Staus")
					if dNode.Status == nil {
						ginkgo.Fail("Driver status is not updated")
					}
					ginkgo.By("Checking to see if node is ReadyForVolumeAllocation")
					if dNode.Status.ReadyForVolumeAllocation == nil || *dNode.Status.ReadyForVolumeAllocation != true {
						ginkgo.Fail("Driver found not ready for allocation")
					}
					ginkgo.By("Checking to see if node reported heartbeat")
					if dNode.Status.LastHeartbeatTime == nil || *dNode.Status.LastHeartbeatTime <= 0 {
						ginkgo.Fail("Driver heartbeat not reported")
					}

					ginkgo.By("Checking to see if node has partition key label.")
					partition, ok := dNode.Labels[partitionKey]
					if ok == false || partition == "" {
						ginkgo.Fail("Driver node parition label was not applied correctly.")
					}
					break
				}
			}
		})
	})
	// ginkgo.Context("AzVolumeAttachment", func() {
	// 	ginkgo.It("Should initialize AzVolumeAttachment object's status and append finalizer and create labels", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		if len(nodes) < 1 {
	// 			ginkgo.Skip("need at least 1 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}

	// 		tpod, cleanups := setUpPod(1, 1)
	// 		for _, cleanup := range cleanups {
	// 			defer cleanup()
	// 		}
	// 		defer tpod.Cleanup()
	// 		framework.ExpectNoError(err)
	// 		pvc, err := cs.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.Background(), tpod.GetPVCName(0), metav1.GetOptions{})
	// 		framework.ExpectNoError(err)

	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		framework.ExpectNoError(err)

	// 		attachmentCount := 0
	// 		for _, attachment := range attachments.Items {
	// 			if strings.EqualFold(attachment.Spec.UnderlyingVolume, pvc.Spec.VolumeName) {
	// 				attachmentCount++
	// 				// confirm that the attachment's status has been initialized
	// 				framework.ExpectNotEqual(attachment.Status, nil)
	// 				// confirm that the attachment's finalizers have been properly added
	// 				framework.ExpectNotEqual(attachment.Finalizers, nil)
	// 				framework.ExpectEqual(len(attachment.Finalizers) > 0, true)
	// 				// confirm taht the attachments' labels have been properly added
	// 				framework.ExpectNotEqual(attachment.Labels, nil)
	// 				framework.ExpectEqual(len(attachment.Labels) > 0, true)
	// 			}
	// 		}
	// 		// confirm that 1 AzVolumeAttachment has been created
	// 		framework.ExpectEqual(attachmentCount, 1)
	// 	})
	// })

	// ginkgo.It("Should delete AzVolumeAttachment object properly", func() {
	// 	skipIfUsingInTreeVolumePlugin()
	// 	skipIfNotUsingCSIDriverV2()
	// 	nodes := testsuites.ListNodeNames(cs)
	// 	if len(nodes) < 1 {
	// 		ginkgo.Skip("need at least 1 nodes to verify the test case. Current node count is %d", len(nodes))
	// 	}
	// 	tpod, cleanups := setUpPod(1, 1)
	// 	// Get all PVCs in the generated namesapce
	// 	pvc, err := cs.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.Background(), tpod.GetPVCName(0), metav1.GetOptions{})
	// 	framework.ExpectNoError(err)
	// 	var azVolumeAttachment v1alpha1.AzVolumeAttachment
	// 	// Get AzVolumeAttachment with specified volume name
	// 	attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 	framework.ExpectNoError(err)

	// 	for _, attachment := range attachments.Items {
	// 		if strings.EqualFold(attachment.Spec.UnderlyingVolume, pvc.Spec.VolumeName) {
	// 			azVolumeAttachment = attachment
	// 		}
	// 	}

	// 	tpod.Cleanup()
	// 	// clean up so that azvolume and azvolumeattachment can be triggered its deletions
	// 	for _, cleanup := range cleanups {
	// 		cleanup()
	// 	}

	// 	conditionFunc := func() (bool, error) {
	// 		_, err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Get(context.Background(), azVolumeAttachment.Name, metav1.GetOptions{})
	// 		if err == nil {
	// 			return false, nil
	// 		}
	// 		if errors.IsNotFound(err) {
	// 			return true, nil
	// 		}

	// 		return false, err
	// 	}

	// 	err = wait.PollImmediate(time.Duration(15)*time.Second, time.Duration(5)*time.Minute, conditionFunc)
	// 	framework.ExpectNoError(err)
	// })

	// ginkgo.It("Should create replica AzVolumeAttachment object when maxShares > 1", func() {
	// 	skipIfUsingInTreeVolumePlugin()
	// 	skipIfNotUsingCSIDriverV2()
	// 	nodes := testsuites.ListNodeNames(cs)
	// 	if len(nodes) < 2 {
	// 		ginkgo.Skip("need at least 2 nodes to verify the test case. Current node count is %d", len(nodes))
	// 	}

	// 	tpod, cleanups := setUpPod(256, 2)
	// 	for _, cleanup := range cleanups {
	// 		defer cleanup()
	// 	}
	// 	defer tpod.Cleanup()

	// 	// Get all PVCs in the generated namesapce
	// 	pvc, err := cs.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.Background(), tpod.GetPVCName(0), metav1.GetOptions{})
	// 	framework.ExpectNoError(err)

	// 	conditionFunc := func() (bool, error) {
	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		if err != nil {
	// 			return false, nil
	// 		}

	// 		replicaCount := 0
	// 		for _, attachment := range attachments.Items {
	// 			if attachment.Spec.UnderlyingVolume == pvc.Spec.VolumeName && attachment.Status != nil && attachment.Status.Role == v1alpha1.ReplicaRole {
	// 				replicaCount++
	// 			}
	// 		}

	// 		return replicaCount == 1, err
	// 	}

	// 	err = wait.PollImmediate(time.Duration(15)*time.Second, time.Duration(5)*time.Minute, conditionFunc)
	// 	framework.ExpectNoError(err)
	// })

	// 	ginkgo.It("If failover happens, should turn replica to primary and create an additional replica for replacment", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		volName := "test-volume"
	// 		if len(nodes) < 3 {
	// 			ginkgo.Skip("need at least 3 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		primaryNode := nodes[0]
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, primaryNode, 1)
	// 		defer testAzAtt.Cleanup()
	// 		_ = testAzAtt.Create()
	// 		err := testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		framework.ExpectNoError(err)

	// 		// fail primary attachment
	// 		testsuites.DeleteTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		defer testsuites.NewTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		framework.ExpectNoError(err)

	// 		err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Delete(context.Background(), testsuites.GetAzVolumeAttachmentName(volName, primaryNode), metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)
	// 		err = testAzAtt.WaitForDelete(primaryNode, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// failover to one of replicas
	// 		var replica *v1alpha1.AzVolumeAttachment
	// 		for _, attachment := range attachments.Items {
	// 			if attachment.Status != nil && attachment.Status.Role == v1alpha1.ReplicaRole {
	// 				replica = &attachment
	// 				break
	// 			}
	// 		}
	// 		promoted := replica.DeepCopy()
	// 		promoted.Spec.RequestedRole = v1alpha1.PrimaryRole
	// 		_, err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Update(context.Background(), promoted, metav1.UpdateOptions{})
	// 		framework.ExpectNoError(err)

	// 		// check if a new primary has been created
	// 		err = testAzAtt.WaitForPrimary(time.Duration(5) * time.Minute)
	// 		framework.ExpectNoError(err)
	// 		// check if the second attachment object was created and marked attached.
	// 		err = testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})

	// 	// this test can fail occasionally  as it waits for the deletion of replica attachment if the test attachment is made on non-test nodes
	// 	ginkgo.It("If a replica is deleted, should create another replica to replace the previous one", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		if len(nodes) < 3 {
	// 			ginkgo.Skip("need at least 3 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		volName := "test-volume"
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, nodes[0], 1)
	// 		defer testAzAtt.Cleanup()
	// 		_ = testAzAtt.Create()
	// 		err := testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		framework.ExpectNoError(err)

	// 		// fail replica attachment
	// 		var replica *v1alpha1.AzVolumeAttachment
	// 		for _, attachment := range attachments.Items {
	// 			if attachment.Status != nil && attachment.Status.Role == v1alpha1.ReplicaRole {
	// 				replica = &attachment
	// 				break
	// 			}
	// 		}
	// 		testsuites.DeleteTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), replica.Spec.NodeName)
	// 		defer testsuites.NewTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), replica.Spec.NodeName)
	// 		framework.ExpectNoError(err)

	// 		err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Delete(context.Background(), replica.Name, metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)
	// 		err = testAzAtt.WaitForDelete(replica.Spec.NodeName, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// check if a new replica has been created
	// 		err = testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})

	// 	// this is a convergeance test to check if number of replicas per volume correctly converges to the right value under certain circumstances (e.g. multiple deletion)
	// 	ginkgo.It("number of replicas should converge to assigned maxMountReplicaCount value", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		volName := "test-volume"
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		if len(nodes) < 3 {
	// 			ginkgo.Skip("need at least 3 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		replicaCount := 2
	// 		primaryNode := nodes[0]
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, primaryNode, replicaCount)
	// 		_ = testAzAtt.Create()
	// 		defer testAzAtt.Cleanup()
	// 		err := testAzAtt.WaitForReplicas(replicaCount, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		framework.ExpectNoError(err)

	// 		// fail replica attachments
	// 		i := 0
	// 		for _, attachment := range attachments.Items {
	// 			if attachment.Status != nil && attachment.Status.Role == v1alpha1.ReplicaRole {
	// 				err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Delete(context.Background(), attachment.Name, metav1.DeleteOptions{})
	// 				framework.ExpectNoError(err)
	// 				// below will be commented out until the controller test uses AzureDiskDriver_v2 running on a separate dedicated namespace for testing
	// 				// err = testsuites.WaitForDelete(azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace), att.Namespace, replica.Name, time.Duration(5)*time.Minute)
	// 				// framework.ExpectNoError(err)
	// 				i++
	// 			}
	// 			if i >= replicaCount {
	// 				break
	// 			}
	// 		}
	// 		time.Sleep(time.Duration(30) * time.Second)

	// 		// check if the number of replicas have properly converged
	// 		err = testAzAtt.WaitForReplicas(replicaCount, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})

	// 	ginkgo.It("should garbage collect replica AzVolumeAttachment if no promotion or new primary creation happens in a given period of time", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		volName := "test-volume"
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		if len(nodes) < 2 {
	// 			ginkgo.Skip("need at least 2 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		replicaCount := 1
	// 		primaryNode := nodes[0]
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, primaryNode, replicaCount)
	// 		primary := testAzAtt.Create()
	// 		defer testAzAtt.Cleanup()
	// 		err := testAzAtt.WaitForReplicas(replicaCount, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// fail primary attachment
	// 		testsuites.DeleteTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		defer testsuites.NewTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		framework.ExpectNoError(err)

	// 		err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Delete(context.Background(), primary.Name, metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)
	// 		err = testAzAtt.WaitForDelete(primaryNode, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 		time.Sleep(time.Duration(5) * time.Minute)

	// 		// check if the replicas and primary had been properly deleted
	// 		for _, node := range nodes {
	// 			err = testAzAtt.WaitForDelete(node, time.Duration(5)*time.Minute)
	// 			framework.ExpectNoError(err)
	// 		}
	// 	})

	// 	ginkgo.It("should not delete replica AzVolumeAttachment if new primary creation happens within a given period of time after primary failure", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		volName := "test-volume"
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		if len(nodes) < 2 {
	// 			ginkgo.Skip("need at least 2 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		replicaCount := 1
	// 		primaryNode := nodes[0]
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, primaryNode, replicaCount)
	// 		_ = testAzAtt.Create()
	// 		defer testAzAtt.Cleanup()
	// 		err := testAzAtt.WaitForReplicas(replicaCount, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// fail primary attachment
	// 		testsuites.DeleteTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		defer testsuites.NewTestAzDriverNode(azDiskClient.DiskV1alpha1().AzDriverNodes(namespace), primaryNode)
	// 		framework.ExpectNoError(err)

	// 		err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Delete(context.Background(), testsuites.GetAzVolumeAttachmentName(volName, primaryNode), metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)
	// 		err = testAzAtt.WaitForDelete(primaryNode, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// failover to one of replicas
	// 		attachments, err := azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).List(context.Background(), metav1.ListOptions{})
	// 		framework.ExpectNoError(err)

	// 		var replica *v1alpha1.AzVolumeAttachment
	// 		for _, attachment := range attachments.Items {
	// 			if attachment.Status != nil && attachment.Status.Role == v1alpha1.ReplicaRole {
	// 				replica = &attachment
	// 				break
	// 			}
	// 		}

	// 		promoted := replica.DeepCopy()
	// 		promoted.Spec.RequestedRole = v1alpha1.PrimaryRole
	// 		_, err = azDiskClient.DiskV1alpha1().AzVolumeAttachments(namespace).Update(context.Background(), promoted, metav1.UpdateOptions{})
	// 		framework.ExpectNoError(err)

	// 		// check if a new primary has been created
	// 		err = testAzAtt.WaitForPrimary(time.Duration(5) * time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// check if the second attachment object was created and marked attached.
	// 		err = testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)

	// 		// sleep for 5 minutes to make sure the replica did not get garbage collected
	// 		time.Sleep(time.Minute * time.Duration(5))
	// 		err = testAzAtt.WaitForReplicas(1, time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})
	// })

	// ginkgo.Context("AzVolumes", func() {
	// 	ginkgo.It("Should initialize AzVolume object", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		testAzVolume := testsuites.SetupTestAzVolume(azDiskClient.DiskV1alpha1(), namespace, "test-volume", 0)
	// 		defer testAzVolume.Cleanup()
	// 		_ = testAzVolume.Create()

	// 		err = testAzVolume.WaitForFinalizer(time.Duration(5) * time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})

	// 	ginkgo.It("Should delete AzVolume object", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		testAzVolume := testsuites.SetupTestAzVolume(azDiskClient.DiskV1alpha1(), namespace, "test-volume-delete", 0)
	// 		defer testAzVolume.Cleanup()
	// 		volume := testAzVolume.Create()

	// 		err = testAzVolume.WaitForFinalizer(time.Duration(5) * time.Minute)
	// 		framework.ExpectNoError(err)
	// 		err = azDiskClient.DiskV1alpha1().AzVolumes(namespace).Delete(context.Background(), volume.Name, metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)

	// 		err = testAzVolume.WaitForDelete(time.Duration(3) * time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})

	// 	ginkgo.It("Should delete AzVolumeAttachment objects with deletion of AzVolume", func() {
	// 		skipIfUsingInTreeVolumePlugin()
	// 		skipIfNotUsingCSIDriverV2()
	// 		nodes := testsuites.ListNodeNames(cs)
	// 		volName := "test-volume-delete"
	// 		if len(nodes) < 1 {
	// 			ginkgo.Skip("need at least 1 nodes to verify the test case. Current node count is %d", len(nodes))
	// 		}
	// 		testAzAtt := testsuites.SetupTestAzVolumeAttachment(azDiskClient.DiskV1alpha1(), namespace, volName, nodes[0], 0)
	// 		_ = testAzAtt.Create()
	// 		defer testAzAtt.Cleanup()

	// 		err = testAzAtt.WaitForAttach(time.Duration(5) * time.Minute)
	// 		framework.ExpectNoError(err)
	// 		time.Sleep(time.Duration(3) * time.Minute)

	// 		// If AzVolume is deleted, AzVolumeAttachment referring to the deleted AzVolume should also be deleted.
	// 		err = azDiskClient.DiskV1alpha1().AzVolumes(namespace).Delete(context.Background(), volName, metav1.DeleteOptions{})
	// 		framework.ExpectNoError(err)

	// 		err = testAzAtt.WaitForDelete(nodes[0], time.Duration(5)*time.Minute)
	// 		framework.ExpectNoError(err)
	// 	})
	// })
}
