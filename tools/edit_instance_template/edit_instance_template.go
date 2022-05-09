package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/golang/protobuf/proto"

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
					Type: computepb.AccessConfig_ONE_TO_ONE_NAT.Enum(),
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
		return fmt.Errorf("Could not create new template: %s", err)
	}
	return nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	c, err := compute.NewInstanceTemplatesRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create instance template client: %s", err)
	}
	defer c.Close()

	newTemplate := *template + "-" + *suffix
	req := &computepb.GetInstanceTemplateRequest{
		InstanceTemplate: newTemplate,
		Project:          *project,
	}
	_, err = c.Get(ctx, req)
	if err != nil {
		if !strings.HasPrefix(err.Error(), "404") {
			log.Fatalf("Could not check for existence of new template: %s", err)
		}
		log.Printf("New template does not exist, creating...")

		if err := createModifiedTemplate(ctx, c, newTemplate); err != nil {
			log.Fatalf("Could not create new template: %s", err)
		}
		log.Printf("New template %q created", newTemplate)
	} else {
		log.Printf("New template %q already exists, not creating.", newTemplate)
	}

	migc, err := compute.NewInstanceGroupManagersRESTClient(ctx)
	if err != nil {
		log.Fatalf("Could not create MIG client: %s", err)
	}
	migsByZone, err := migc.AggregatedList(ctx, &computepb.AggregatedListInstanceGroupManagersRequest{
		Project: *project,
	})
	if err != nil {
		log.Fatalf("Could not retrieve MIG list: %s", err)
	}
	for _, migs := range migsByZone.GetItems() {
		for _, mig := range migs.GetInstanceGroupManagers() {
			if strings.HasSuffix(mig.GetInstanceTemplate(), "/"+*template) {
				// GKE doesn't allow us to add network interfaces attached to different networks for a existing instance group. Therefore, we have to delete the instance group and then create with the new template
				log.Printf("Command to delete and create instance groups:")
				fmt.Printf("gcloud compute instance-groups managed delete %s --zone %s\n", mig.GetName(), mig.GetZone())
				fmt.Printf("gcloud compute instance-groups managed create %s --template %s --zone %s --size %d\n", mig.GetName(), newTemplate, mig.GetZone(), *size)
			}
		}
	}
}
