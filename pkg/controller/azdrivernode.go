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

package controller

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	azVolumeClientSet "sigs.k8s.io/azuredisk-csi-driver/pkg/apis/client/clientset/versioned"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ReconcileAzDriverNode reconciles AzDriverNode
type ReconcileAzDriverNode struct {
	client client.Client

	azVolumeClient azVolumeClientSet.Interface

	namespace string
}

// Implement reconcile.Reconciler so the controller can reconcile objects
var _ reconcile.Reconciler = &ReconcileAzDriverNode{}

func (r *ReconcileAzDriverNode) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	klog.V(2).Info("Checking to see if node (%v) exists.", request.NamespacedName)
	n := &corev1.Node{}
	err := r.client.Get(ctx, request.NamespacedName, n)

	// If the node still exists don't delete the AzDriverNode
	if err == nil {
		klog.Errorf("Node still exists. Skip deleting azDriverNode for Node: (%v)", n)
		return reconcile.Result{}, nil
	}

	// If the node is not found, delete the corresponding AzDriverNode
	if errors.IsNotFound(err) {

		klog.V(2).Info("Deleting AzDriverNode (%s).", request.Name)

		// Delete the azDriverNode, since corresponding node is deleted
		azN := r.azVolumeClient.DiskV1alpha1().AzDriverNodes(r.namespace)
		err = azN.Delete(ctx, request.Name, metav1.DeleteOptions{})

		// If there is an issue in deleteing the AzDriverNode, requeue
		if err != nil && !errors.IsNotFound(err) {
			klog.Errorf("Will retry. Failed to delete AzDriverNode: (%s). Error: (%v)", request.Name, err)
			return reconcile.Result{Requeue: true}, nil
		}

		// Delete all volumeAttachments attached to this node, if failed, requeue
		if err = cleanUpAzVolumeAttachmentByNode(ctx, r.client, r.azVolumeClient, r.namespace, request.Name); err != nil {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, nil
	}

	klog.Errorf("Failed to query node. Error: %v. Will retry...", err)

	return reconcile.Result{Requeue: true}, err
}

func (r *ReconcileAzDriverNode) cleanUpAzDriverNodes(ctx context.Context) error {
	azDriverNodes, err := r.azVolumeClient.DiskV1alpha1().AzDriverNodes(r.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list AzDriverNodes: %v", err)
		return err
	}

	for _, azDriverNode := range azDriverNodes.Items {
		if err := r.client.Delete(ctx, &azDriverNode, &client.DeleteOptions{}); err != nil {
			klog.Errorf("failed to delete AzDriverNodes (%s): %v", azDriverNode.Name, err)
			return err
		}
	}

	return nil
}

func (r *ReconcileAzDriverNode) MonitorAndCleanUp(ctx context.Context, wg *sync.WaitGroup) {
	// start a separate goroutine to monitor context cancellation. clean up upon cancellation
	wg.Add(1)
	go func(wg *sync.WaitGroup, ctx context.Context) {
		defer wg.Done()
		<-ctx.Done()
		_ = r.cleanUpAzDriverNodes(context.TODO())
	}(wg, ctx)
}

// NewAzDriverNodeController initializes azdrivernode-controller
func NewAzDriverNodeController(mgr manager.Manager, azVolumeClient *azVolumeClientSet.Interface, namespace string) (*ReconcileAzDriverNode, error) {
	logger := mgr.GetLogger().WithValues("controller", "azdrivernode")
	reconciler := ReconcileAzDriverNode{client: mgr.GetClient(), azVolumeClient: *azVolumeClient, namespace: namespace}
	c, err := controller.New("azdrivernode-controller", mgr, controller.Options{
		MaxConcurrentReconciles: 10,
		Reconciler:              &reconciler,
		Log:                     logger,
	})

	if err != nil {
		klog.Errorf("Failed to create azdrivernode controller. Error: %v", err)
		return nil, err
	}

	// Predicate to only reconcile deleted nodes
	p := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
	}

	klog.V(2).Info("Starting to watch cluster nodes.")
	// Watch the nodes
	err = c.Watch(&source.Kind{Type: &corev1.Node{}}, &handler.EnqueueRequestForObject{}, p)
	if err != nil {
		klog.Errorf("Failed to watch nodes. Error: %v", err)
		return nil, err
	}
	klog.V(2).Info("Controller set-up successfull.")

	return &reconciler, err
}
