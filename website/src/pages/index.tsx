import Layout from "@theme/Layout";
import React from "react";
import common from "../css/common.module.css";

import Brands from "../components/brands/brands";
import CTA from "../components/cta/cta";
import Customers from "../components/customers/customers";
import Enterprise from "../components/enterprise/enterprise";
import Hero from "../components/hero/hero";
import Integrations from "../components/integrations/integrations";
import Logs from "../components/logs/logs";
import OSS from "../components/oss/oss";
import Quote from "../components/quote/quote";
import RBE from "../components/rbe/rbe";

function Index() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero image={require("../../static/img/ui.png")} bigImage={true} lessPadding={true} gradientButton={true} />
        <Customers />
        <RBE />
        <Logs />
        <Enterprise />
        <Quote />
        <Integrations />
        <OSS />
        <CTA />
        <Brands />
      </div>
    </Layout>
  );
}

export default Index;
