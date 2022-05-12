package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"google.golang.org/api/iterator"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
)

var (
	project = flag.String("project", "", "GCP project that hosts the template")
	region  = flag.String("region", "", "The region of the subnetwork")
	cluster = flag.String("cluster", "", "The cluster to modify")
	pool    = flag.String("pool", "executor-pool", "The pool to modify")

	subnetwork = flag.String("subnetwork", "executor", "The name of the subnetwork")
	apply      = flag.Bool("apply", false, "If false, run this script in dry-run mode (makes no changes)")
)

func getSubnetURL() string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", *project, *region, *subnetwork)
}

func findInstanceTemplates(ctx context.Context, c *compute.InstanceTemplatesClient) ([]*computepb.InstanceTemplate, error) {
	it := c.List(ctx, &computepb.ListInstanceTemplatesRequest{
		Filter:  proto.String(fmt.Sprintf("name = gke-%s-%s-*", *cluster, *pool)),
		Project: *project,
	})

	templates := make([]*computepb.InstanceTemplate, 0)
	for {
		tpl, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		templates = append(templates, tpl)
	}
	return templates, nil
}

func alreadyModified(t *computepb.InstanceTemplate) bool {
	if !t.GetProperties().GetAdvancedMachineFeatures().GetEnableNestedVirtualization() {
		log.Printf("nested virt was not enabled on %q", t.GetName())
		return false
	}

	foundOneToOneNAT := false
	for _, ni := range t.GetProperties().GetNetworkInterfaces() {
		if ni.GetSubnetwork() != getSubnetURL() {
			continue
		}
		for _, ac := range ni.GetAccessConfigs() {
			if ac.GetType() == "ONE_TO_ONE_NAT" {
				foundOneToOneNAT = true
			}
		}
	}
	if !foundOneToOneNAT {
		log.Printf("ONE_TO_ONE_NAT not found on %q", t.GetName())
		return false
	}
	return true
}

func modifiedTemplateName(unmodifiedName string) (string, error) {
	const sep = "-"
	bits := strings.Split(unmodifiedName, sep)
	if bits[len(bits)-2] == "bb" {
		// increment this template generation by 1.
		i, err := strconv.Atoi(bits[len(bits)-1])
		if err != nil {
			return "", err
		}
		bits[len(bits)-1] = strconv.Itoa(i + 1)
		return strings.Join(bits, sep), nil
	}
	return unmodifiedName + sep + "bb-1", nil
}

func createModifiedVersion(ctx context.Context, c *compute.InstanceTemplatesClient, old *computepb.InstanceTemplate, newTemplateName string) (*computepb.InstanceTemplate, error) {
	new := proto.Clone(old).(*computepb.InstanceTemplate)
	if new.GetProperties().GetAdvancedMachineFeatures() == nil {
		new.GetProperties().AdvancedMachineFeatures = &computepb.AdvancedMachineFeatures{}
	}
	new.GetProperties().GetAdvancedMachineFeatures().EnableNestedVirtualization = proto.Bool(true)
	new.GetProperties().NetworkInterfaces = append(new.GetProperties().GetNetworkInterfaces(),
		&computepb.NetworkInterface{
			AccessConfigs: []*computepb.AccessConfig{
				{
					Name: proto.String("external-nat"),
					Type: proto.String("ONE_TO_ONE_NAT"),
				},
			},
			Subnetwork: proto.String(getSubnetURL()),
		})
	new.Name = proto.String(newTemplateName)

	req := &computepb.InsertInstanceTemplateRequest{
		Project:                  *project,
		InstanceTemplateResource: new,
	}
	if !*apply {
		// Don't print the template here, it's fucken huge.
		log.Printf("Would create new template: %q", newTemplateName)
		return nil, nil
	}
	log.Printf("Modifying template %q to create %q...", old.GetName(), newTemplateName)
	_, err := c.Insert(ctx, req)
	if err != nil {
		return nil, err
	}

	getReq := &computepb.GetInstanceTemplateRequest{
		InstanceTemplate: newTemplateName,
		Project:          *project,
	}
	return c.Get(ctx, getReq)
}

func getIGMs(ctx context.Context, c *compute.InstanceGroupManagersClient) ([]*computepb.InstanceGroupManager, error) {
	igms := make([]*computepb.InstanceGroupManager, 0)
	it := c.AggregatedList(ctx, &computepb.AggregatedListInstanceGroupManagersRequest{
		Project: *project,
	})
	for {
		rsp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, igm := range rsp.Value.GetInstanceGroupManagers() {
			shortZone := path.Base(igm.GetZone())
			if !strings.HasPrefix(shortZone, *region) {
				continue
			}
			igms = append(igms, igm)
		}
	}
	return igms, nil
}

func createIGMWithTemplate(ctx context.Context, c *compute.InstanceGroupManagersClient, name, shortZone, modifiedTemplateURL string) error {
	req := &computepb.InsertInstanceGroupManagerRequest{
		Project: *project,
		InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name:             proto.String(name),
			InstanceTemplate: proto.String(modifiedTemplateURL),
			TargetSize:       proto.Int32(int32(1)),
		},
		Zone: shortZone,
	}

	if !*apply {
		log.Printf("Would create InstanceGroupManager (%+v)", req)
		return nil
	}
	log.Printf("Creating InstanceGroupManager (%+v)...", req)
	op, err := c.Insert(ctx, req)
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func deleteIGM(ctx context.Context, c *compute.InstanceGroupManagersClient, igm *computepb.InstanceGroupManager) error {
	req := &computepb.DeleteInstanceGroupManagerRequest{
		Project:              *project,
		InstanceGroupManager: igm.GetName(),
		Zone:                 path.Base(igm.GetZone()),
	}

	if !*apply {
		log.Printf("Would delete InstanceGroupManager (%+v)", req)
		return nil
	}
	log.Printf("Deleting InstanceGroupManager (%+v)...", req)
	op, err := c.Delete(ctx, req)
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func main() {
	flag.Parse()

	if !*apply {
		defer log.Printf("Run this script again with --apply=true to execute these changes!")
	}

	if *region == "" || *cluster == "" {
		log.Fatalf("Both --region and --cluster flags must be set.")
	}
	ctx := context.Background()
	c, err := compute.NewInstanceTemplatesRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create instance template client: %s", err)
	}
	defer c.Close()

	migc, err := compute.NewInstanceGroupManagersRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create MIG client: %s", err)
	}
	defer migc.Close()

	// Find all instance templates that match our cluster and pool.
	templates, err := findInstanceTemplates(ctx, c)
	if err != nil {
		log.Fatalf("Error fetching instance templates: %s", err)
	}

	// Seperate the templates into those that have already been modified
	// and those that still need to be modified to enable our features.
	modified := make(map[string]*computepb.InstanceTemplate, 0)
	unmodified := make(map[string]*computepb.InstanceTemplate, 0)
	for _, t := range templates {
		name := t.GetName()
		if alreadyModified(t) {
			modified[name] = t
		} else {
			unmodified[name] = t
		}
	}

	modifiableCount := 0
	// Modify any templates as necessary.
	for name := range unmodified {
		modifiedName, err := modifiedTemplateName(name)
		if err != nil {
			log.Fatalf("Error computing modifed template name for %q: %s", name, err)
		}
		if _, ok := modified[modifiedName]; ok {
			log.Printf("a modified version of %q already exists (%q). skipping!", name, modifiedName)
			continue
		}
		modifiableCount += 1
		new, err := createModifiedVersion(ctx, c, unmodified[name], modifiedName)
		if err != nil {
			log.Fatalf("error modifying template %q: %s", name, err)
		}
		modified[modifiedName] = new
	}

	log.Printf("%d templates already modified, %d to modify", len(modified), modifiableCount)

	igms, err := getIGMs(ctx, migc)
	if err != nil {
		log.Fatalf("error fetching managed instance groups: %s", err)
	}

	// at this point, every template should at least have a "modified"
	// version that enables our extra features.
	//
	// now we go through and ensure every modified version has an active
	// instancegroupmanager using it; and every unmodified version does
	// not.
	for name := range unmodified {
		modifiedName, err := modifiedTemplateName(name)
		if err != nil {
			log.Fatalf("Error computing modifed template name for %q: %s", name, err)
		}

		unmodifiedTemplateURL := unmodified[name].GetSelfLink()
		modifiedTemplateURL := modified[modifiedName].GetSelfLink()

		for _, igm := range igms {
			if igm.GetInstanceTemplate() == unmodifiedTemplateURL {
				if err := deleteIGM(ctx, migc, igm); err != nil {
					log.Fatalf("Error deleting instance group manager: %s", err)
				}
				shortZone := path.Base(igm.GetZone())
				if err := createIGMWithTemplate(ctx, migc, igm.GetName(), shortZone, modifiedTemplateURL); err != nil {
					log.Fatalf("Error creating instance group manager: %s", err)
				}
			}
		}
	}
}
