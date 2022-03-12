import React from "react";
import Layout from "@theme/Layout";
import common from "../css/common.module.css";

import Hero from "../components/hero/hero";
import Brands from "../components/brands/brands";
import RBE from "../components/rbe/rbe";
import Logs from "../components/logs/logs";
import Enterprise from "../components/enterprise/enterprise";
import Quote from "../components/quote/quote";
import Integrations from "../components/integrations/integrations";
import OSS from "../components/oss/oss";
import CTA from "../components/cta/cta";

function Index() {
  return (
    <Layout title="Bazel at Enterprise Scale">
      <div className={common.page}>
        <Hero image={require("../../static/img/ui.png")} bigImage={true} lessPadding={true} />
        <Brands />
        <RBE />
        <Logs />
        <Enterprise />
        <Quote />
        <Integrations />
        <OSS />
        <CTA />
      </div>
    </Layout>
  );
}

export default Index;
