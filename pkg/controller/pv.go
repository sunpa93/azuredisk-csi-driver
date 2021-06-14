/*
Copyright 2017 The Kubernetes Authors.

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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/apis/azuredisk/v1alpha1"
	azVolumeClientSet "sigs.k8s.io/azuredisk-csi-driver/pkg/apis/client/clientset/versioned"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

//Struct for the reconciler
type reconcilePV struct {
	client         client.Client
	azVolumeClient azVolumeClientSet.Interface
	namespace      string
}

// Implement reconcile.Reconciler so the controller can reconcile objects
var _ reconcile.Reconciler = &reconcilePV{}

func (r *reconcilePV) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	var pv corev1.PersistentVolume
	if err := r.client.Get(ctx, request.NamespacedName, &pv); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
		klog.Errorf("failed to get PV (%s): %v", request.Name, err)
		return reconcile.Result{Requeue: true}, err
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != azureutils.DriverName {
		return reconcile.Result{}, nil
	}

	var azVolume v1alpha1.AzVolume
	diskName, err := azureutils.GetDiskNameFromAzureManagedDiskURI(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		klog.Errorf("failed to extract proper diskName from pv(%s)'s volume handle (%s): %v", pv.Name, pv.Spec.CSI.VolumeHandle, err)
		// if disk name cannot be extracted from volumehandle, there is no point of requeueing
		return reconcile.Result{}, err
	}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: r.namespace, Name: diskName}, &azVolume); err != nil {
		// if underlying PV was found but AzVolume was not. Might be due to stale cache
		klog.V(5).Infof("failed to get AzVolume (%s): %v", diskName, err)
		return reconcile.Result{}, nil
	}

	if azVolume.Status.Detail != nil {
		if pv.Status.Phase == corev1.VolumeReleased && azVolume.Status.Detail.Phase == v1alpha1.VolumeBound {
			updated := azVolume.DeepCopy()
			updated.Status.Detail.Phase = v1alpha1.VolumeReleased

			if err := r.client.Update(ctx, updated, &client.UpdateOptions{}); err != nil {
				klog.Errorf("failed to update AzVolume (%s): %v", pv.Name, err)
				return reconcile.Result{Requeue: true}, err
			}
		}
	}
	return reconcile.Result{}, nil
}

func NewPVController(mgr manager.Manager, azVolumeClient *azVolumeClientSet.Interface, namespace string) error {
	logger := mgr.GetLogger().WithValues("controller", "azvolume")

	c, err := controller.New("pv-controller", mgr, controller.Options{
		MaxConcurrentReconciles: 10,
		Reconciler:              &reconcilePV{client: mgr.GetClient(), azVolumeClient: *azVolumeClient, namespace: namespace},
		Log:                     logger,
	})

	if err != nil {
		klog.Errorf("Failed to create azvolume controller. Error: (%v)", err)
		return err
	}

	klog.V(2).Info("Starting to watch PV.")

	p := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
	}

	// Watch for Update events on PV objects
	err = c.Watch(&source.Kind{Type: &corev1.PersistentVolume{}}, &handler.EnqueueRequestForObject{}, p)
	if err != nil {
		klog.Errorf("Failed to watch PV. Error: %v", err)
		return err
	}

	klog.V(2).Info("Controller set-up successful.")
	return nil
}
