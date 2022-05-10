package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/golang/protobuf/proto"
	"google.golang.org/api/iterator"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
)

var (
	project    = flag.String("project", "", "GCP project that hosts the template")
	template   = flag.String("template", "", "The name of the base template")
	suffix     = flag.String("suffix", "ssd-network", "Suffix to append to instance template name")
	size       = flag.Int("size", 5, "The initial number of instances in the group")
	region     = flag.String("region", "us-west1", "The region of the subnetwork")
	subnetwork = flag.String("subnetwork", "executor", "The name of the subnetwork")
)

func createModifiedTemplate(ctx context.Context, c *compute.InstanceTemplatesClient, newTemplateName string) error {
	req := &computepb.GetInstanceTemplateRequest{
		InstanceTemplate: *template,
		Project:          *project,
	}
	resp, err := c.Get(ctx, req)
	if err != nil {
		return fmt.Errorf("could not retrieve base template %q: %s", *template, err)
	}
	if resp.GetProperties().GetAdvancedMachineFeatures() == nil {
		resp.GetProperties().AdvancedMachineFeatures = &computepb.AdvancedMachineFeatures{}
	}
	subnetworkURL := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", *project, *region, *subnetwork)

	resp.GetProperties().GetAdvancedMachineFeatures().EnableNestedVirtualization = proto.Bool(true)
	resp.GetProperties().NetworkInterfaces = append(resp.GetProperties().GetNetworkInterfaces(),
		&computepb.NetworkInterface{
			AccessConfigs: []*computepb.AccessConfig{
				{
					Name: proto.String("external-nat"),
					Type: proto.String("ONE_TO_ONE_NAT"),
				},
			},
			Subnetwork: proto.String(subnetworkURL),
		})
	resp.Name = proto.String(newTemplateName)

	createReq := &computepb.InsertInstanceTemplateRequest{
		Project:                  *project,
		InstanceTemplateResource: resp,
	}
	_, err = c.Insert(ctx, createReq)
	if err != nil {
		return err
	}
	return nil
}

func getTemplateURL(ctx context.Context, c *compute.InstanceTemplatesClient, templateName string) (string, error) {
	req := &computepb.GetInstanceTemplateRequest{
		InstanceTemplate: templateName,
		Project:          *project,
	}
	rsp, err := c.Get(ctx, req)
	if err != nil {
		return "", err
	}
	return rsp.GetSelfLink(), nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	c, err := compute.NewInstanceTemplatesRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create instance template client: %s", err)
	}
	defer c.Close()

	newTemplateURL := ""
	newTemplateName := *template + "-" + *suffix
	if n, err := getTemplateURL(ctx, c, newTemplateName); err != nil {
		if !strings.Contains(err.Error(), "Error 404") {
			log.Fatalf("Could not check for existence of new template: %s", err)
		}
		log.Printf("New template does not exist, creating...")

		if err := createModifiedTemplate(ctx, c, newTemplateName); err != nil {
			log.Fatalf("Could not create new template: %s", err)
		}
		log.Printf("New template %q created", newTemplateName)
		if n2, err := getTemplateURL(ctx, c, newTemplateName); err != nil {
			log.Fatalf("Error getting new template URL: %s", err)
		} else {
			newTemplateURL = n2
		}
	} else {
		newTemplateURL = n
		log.Printf("New template %q already exists, not creating.", newTemplateName)
	}

	migc, err := compute.NewInstanceGroupManagersRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create MIG client: %s", err)
	}
	it := migc.AggregatedList(ctx, &computepb.AggregatedListInstanceGroupManagersRequest{
		Project: *project,
	})

	migsByZone := make(map[string]*computepb.InstanceGroupManagersScopedList)
	for {
		rsp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Could not retrieve MIG list: %s", err)
		}
		migsByZone[rsp.Key] = rsp.Value
	}

	for _, migs := range migsByZone {
		for _, mig := range migs.GetInstanceGroupManagers() {
			if strings.HasSuffix(mig.GetInstanceTemplate(), "/"+*template) {
				// GKE doesn't allow us to add network
				// interfaces attached to different networks for
				// an existing instance group. Therefore, we
				// have to delete the instance group and then
				// create with the new template
				shortZone := path.Base(mig.GetZone())
				op, err := migc.Delete(ctx, &computepb.DeleteInstanceGroupManagerRequest{
					Project:              *project,
					InstanceGroupManager: mig.GetName(),
					Zone:                 shortZone,
				})
				if err != nil {
					log.Fatalf("Error deleting old instance group %s: %s", mig.GetName(), err)
				}
				if err := op.Wait(ctx); err != nil {
					log.Fatalf("Error waiting for old instance group to be deleted %s: %s", mig.GetName(), err)
				}

				op, err = migc.Insert(ctx, &computepb.InsertInstanceGroupManagerRequest{
					Project: *project,
					InstanceGroupManagerResource: &computepb.InstanceGroupManager{
						Name:             proto.String(mig.GetName()),
						InstanceTemplate: proto.String(newTemplateURL),
						TargetSize:       proto.Int32(int32(*size)),
					},
					Zone: shortZone,
				})
				if err != nil {
					log.Fatalf("Error creating new instance group %s: %s", mig.GetName(), err)
				}
				if err := op.Wait(ctx); err != nil {
					log.Fatalf("Error waiting for new instance group to be created %s: %s", mig.GetName(), err)
				}
			}
		}
	}
}
