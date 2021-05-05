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
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/apis/azuredisk/v1alpha1"
	azVolumeClientSet "sigs.k8s.io/azuredisk-csi-driver/pkg/apis/client/clientset/versioned"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type reconcileVolumeAttachment struct {
	client         client.Client
	azVolumeClient azVolumeClientSet.Interface
	namespace      string
}

var _ reconcile.Reconciler = &reconcileVolumeAttachment{}

func (r *reconcileVolumeAttachment) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	var volumeAttachment storagev1.VolumeAttachment
	if err := r.client.Get(ctx, request.NamespacedName, &volumeAttachment); err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("failed to get VolumeAttachment (%s): %v", request.Name, err)
			return reconcile.Result{Requeue: true}, err
		}
		return reconcile.Result{}, nil
	}

	volumeAttachmentExists := true
	if now := metav1.Now(); volumeAttachment.DeletionTimestamp.Before(&now) {
		volumeAttachmentExists = false
	}

	volumeName := volumeAttachment.Spec.Source.PersistentVolumeName
	if volumeName == nil {
		return reconcile.Result{Requeue: false}, status.Error(codes.Aborted, fmt.Sprintf("PV name is set nil for VolumeAttachment (%s)", volumeAttachment.Name))
	}
	nodeName := volumeAttachment.Spec.NodeName
	if err := r.AnnotateAzVolumeAttachment(ctx, azureutils.GetAzVolumeAttachmentName(*volumeName, nodeName), volumeAttachmentExists); err != nil {
		return reconcile.Result{Requeue: true}, err
	}
	return reconcile.Result{}, nil
}

func (r *reconcileVolumeAttachment) AnnotateAzVolumeAttachment(ctx context.Context, azVolumeAttachmentName string, volumeAttachmentExists bool) error {
	var azVolumeAttachment v1alpha1.AzVolumeAttachment
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: r.namespace, Name: azVolumeAttachmentName}, &azVolumeAttachment); err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("failed to get AzVolumeAttachment (%s): %v", azVolumeAttachmentName, err)
			return err
			// if the AzVolumeAttachment object has already been deleted, ignore
		} else {
			klog.V(5).Infof("AzVolumeAttachment (%s) already deleted", azVolumeAttachmentName)
			return nil
		}
	}

	updated := azVolumeAttachment.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	// avoid unnecessary update
	if vaExists, ok := updated.Annotations[azureutils.VolumeAttachmentExistsAnnotation]; ok && vaExists == strconv.FormatBool(volumeAttachmentExists) {
		return nil
	}
	updated.Annotations[azureutils.VolumeAttachmentExistsAnnotation] = strconv.FormatBool(volumeAttachmentExists)

	if err := r.client.Update(ctx, updated, &client.UpdateOptions{}); err != nil {
		klog.Errorf("failed to update AzVolumeAttachment (%s): %v", azVolumeAttachmentName, err)
		return err
	}
	klog.V(2).Infof("successfully updated AzVolumeAttachment (%s) with annotation (%s)", azVolumeAttachmentName, azureutils.VolumeAttachmentExistsAnnotation)
	return nil
}

func NewVolumeAttachmentController(ctx context.Context, mgr manager.Manager, azVolumeClient *azVolumeClientSet.Interface, namespace string) error {
	reconciler := reconcileAzVolumeAttachment{
		client:         mgr.GetClient(),
		azVolumeClient: *azVolumeClient,
		namespace:      namespace,
	}

	c, err := controller.New("volumeattachment-controller", mgr, controller.Options{
		MaxConcurrentReconciles: 10,
		Reconciler:              &reconciler,
		Log:                     mgr.GetLogger().WithValues("controller", "volumeattachment"),
	})

	if err != nil {
		klog.Errorf("failed to create a new volumeattachment controller: %v", err)
		return err
	}

	// Watch for CRUD events on VolumeAttachment objects
	err = c.Watch(&source.Kind{Type: &storagev1.VolumeAttachment{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		klog.Errorf("failed to initialize watch for azvolumeattachment object: %v", err)
		return err
	}

	klog.V(2).Info("VolumeAttachment Controller successfully initialized.")
	return nil
}
